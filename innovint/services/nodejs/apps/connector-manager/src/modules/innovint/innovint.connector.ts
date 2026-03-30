/**
 * InnoVint Connector Module for PipesHub Connector Manager
 *
 * This module registers the InnoVint connector with PipesHub's Node.js
 * Connector Manager service.  It handles:
 *   - REST endpoints for connector configuration (save / retrieve / delete)
 *   - Encrypted credential storage in ETCD
 *   - Sync-trigger events published to the `sync-events` Kafka topic
 *   - Health / status endpoint
 *
 * ── Playbook-compliant architecture ────────────────────────────────────────
 *
 * Kafka topics used:
 *   sync-events   (outbound) — instructs the Python connector to perform a sync
 *
 * Event format on sync-events:
 *   { event: "innovint.resync", orgId: "…", syncType: "full"|"incremental" }
 *
 * Credential flow:
 *   1. Admin POSTs config → stored in ETCD at
 *      services/connectors/innovint/{orgId}/config
 *   2. A `innovint.resync` event is published to `sync-events`
 *   3. Python connector receives event, calls ConfigurationService.get_config()
 *      to read credentials directly from ETCD — NO HTTP credential callback
 *      (credentialsRoute) is used; that pattern is NOT part of the playbook.
 *
 * ETCD key layout:
 *   services/connectors/innovint/{orgId}/config   — full config + credentials
 *
 * Architecture:
 *   Admin UI → POST /config → ETCD → Kafka sync-events → Python connector
 */

import { Router, Request, Response } from 'express';

// ── Types ────────────────────────────────────────────────────────────────────

/** Config stored in ETCD per org. */
interface InnovintConfig {
  tenant: string;
  apiKey: string;
  enabledEntities: string[];
  syncIntervalMinutes: number;
}

interface InnovintConnectorStatus {
  connectorType: string;
  configured: boolean;
  lastSyncAt?: string;
  enabledEntities: string[];
}

/** Body accepted by POST /sync */
interface SyncRequest {
  orgId: string;
  syncType?: 'full' | 'incremental';
}

// ── Constants ────────────────────────────────────────────────────────────────

const VALID_ENTITY_TYPES = [
  'lots',
  'vessels',
  'analyses',
  'work_orders',
  'case_goods',
  'costs',
] as const;

const DEFAULT_ENABLED_ENTITIES = [...VALID_ENTITY_TYPES];
const DEFAULT_SYNC_INTERVAL_MINUTES = 60;

/** Kafka topic the Python connector listens on for sync triggers. */
const SYNC_EVENTS_TOPIC = 'sync-events';

/** Event name the Python connector's @ConnectorBuilder handler matches. */
const RESYNC_EVENT = 'innovint.resync';

// ── Helpers ──────────────────────────────────────────────────────────────────

/** ETCD key where the Python connector reads credentials via ConfigurationService. */
function etcdConfigKey(orgId: string): string {
  return `services/connectors/innovint/${orgId}/config`;
}

/** Publish a resync event to the sync-events Kafka topic. */
async function publishResyncEvent(
  kafkaProducer: any,
  orgId: string,
  syncType: 'full' | 'incremental',
): Promise<void> {
  await kafkaProducer.send({
    topic: SYNC_EVENTS_TOPIC,
    messages: [
      {
        key: orgId,
        value: JSON.stringify({
          event:    RESYNC_EVENT,
          orgId,
          syncType,
          triggeredAt: new Date().toISOString(),
        }),
      },
    ],
  });
}

// ── Connector Metadata ───────────────────────────────────────────────────────

/**
 * Connector metadata surfaced in PipesHub's connector marketplace / admin UI.
 */
export const INNOVINT_CONNECTOR_METADATA = {
  id:          'innovint',
  name:        'InnoVint',
  displayName: 'InnoVint Winery Management',
  description:
    'Connect your InnoVint winery operating system to ingest production data ' +
    'including lots, vessels, lab analyses, work orders, case goods, and costs.',
  icon:        'innovint-icon.svg',
  category:    'industry-specific',
  subcategory: 'wine-production',
  authMethods: ['api_key'],
  supportedEntities: VALID_ENTITY_TYPES,
  features: {
    fullSync:        true,
    incrementalSync: true,
    realTimeSync:    false,
    webhooks:        false,
    permissions:     true,  // READER/OWNER edges written to ArangoDB
  },
  documentation: 'https://support.innovint.us/hc/en-us',
  configSchema: {
    type:     'object',
    required: ['tenant', 'apiKey'],
    properties: {
      tenant: {
        type:        'string',
        title:       'InnoVint Tenant',
        description: 'Your InnoVint subdomain (e.g. "acme" in acme.innovint.us)',
      },
      apiKey: {
        type:        'string',
        title:       'API Key',
        description: 'Your InnoVint API key',
        format:      'password',
      },
      enabledEntities: {
        type:    'array',
        items:   { type: 'string', enum: VALID_ENTITY_TYPES },
        title:   'Data Types to Sync',
        default: DEFAULT_ENABLED_ENTITIES,
      },
      syncIntervalMinutes: {
        type:    'number',
        title:   'Sync Interval (minutes)',
        minimum: 15,
        maximum: 1440,
        default: DEFAULT_SYNC_INTERVAL_MINUTES,
      },
    },
  },
};

// ── Router Factory ───────────────────────────────────────────────────────────

/**
 * Returns the Express router for InnoVint connector REST endpoints.
 *
 * Mounted under /api/v1/connectors/innovint by the Connector Manager service.
 *
 * @param deps  Injected infrastructure dependencies.
 */
export function createInnovintRouter(deps: {
  /** ETCD client (encrypted key-value store) — used via ConfigurationService pattern. */
  configStore: {
    get(key: string): Promise<string | null>;
    put(key: string, value: string): Promise<void>;
    delete(key: string): Promise<void>;
  };
  /** Kafka producer — publishes to sync-events. */
  kafkaProducer: {
    send(payload: { topic: string; messages: Array<{ key: string; value: string }> }): Promise<void>;
  };
}): Router {
  const router = Router();

  // ── GET /config ──────────────────────────────────────────────────────────
  // Return the saved configuration (with credentials masked).
  router.get('/config', async (req: Request, res: Response) => {
    try {
      const orgId: string = (req as any).orgId;
      const raw = await deps.configStore.get(etcdConfigKey(orgId));

      if (!raw) {
        return res.json({ configured: false });
      }

      const config: InnovintConfig = JSON.parse(raw);

      // Mask the API key — only return the last 4 characters.
      const safeConfig = {
        ...config,
        apiKey: config.apiKey
          ? '••••••••' + config.apiKey.slice(-4)
          : undefined,
      };

      return res.json({ configured: true, config: safeConfig });
    } catch (err) {
      console.error('[InnoVint] GET /config error:', err);
      return res.status(500).json({ error: 'Failed to fetch configuration' });
    }
  });

  // ── POST /config ─────────────────────────────────────────────────────────
  // Save configuration to ETCD and trigger an initial full sync.
  router.post('/config', async (req: Request, res: Response) => {
    try {
      const orgId: string = (req as any).orgId;
      const body = req.body;

      // Validate required fields
      if (!body.tenant || typeof body.tenant !== 'string') {
        return res.status(400).json({ error: 'tenant is required' });
      }
      if (!body.apiKey || typeof body.apiKey !== 'string') {
        return res.status(400).json({ error: 'apiKey is required' });
      }

      // Validate entity types
      const enabledEntities: string[] = body.enabledEntities ?? DEFAULT_ENABLED_ENTITIES;
      const invalid = enabledEntities.filter(
        (e) => !(VALID_ENTITY_TYPES as readonly string[]).includes(e),
      );
      if (invalid.length > 0) {
        return res
          .status(400)
          .json({ error: `Invalid entity types: ${invalid.join(', ')}` });
      }

      const config: InnovintConfig = {
        tenant:              body.tenant,
        apiKey:              body.apiKey,
        enabledEntities,
        syncIntervalMinutes: body.syncIntervalMinutes ?? DEFAULT_SYNC_INTERVAL_MINUTES,
      };

      // Persist to ETCD — the Python connector reads this path via
      // ConfigurationService.get_config() during init() and run_sync().
      await deps.configStore.put(etcdConfigKey(orgId), JSON.stringify(config));

      // Publish a full sync trigger on the playbook-correct Kafka topic.
      await publishResyncEvent(deps.kafkaProducer, orgId, 'full');

      return res.json({
        success: true,
        message: 'InnoVint connector configured; full sync triggered.',
      });
    } catch (err) {
      console.error('[InnoVint] POST /config error:', err);
      return res.status(500).json({ error: 'Failed to save configuration' });
    }
  });

  // ── POST /sync ───────────────────────────────────────────────────────────
  // Manually trigger a sync (full or incremental).
  router.post('/sync', async (req: Request, res: Response) => {
    try {
      const orgId: string = (req as any).orgId;
      const { syncType = 'incremental' } = req.body as SyncRequest;

      if (!['full', 'incremental'].includes(syncType)) {
        return res.status(400).json({ error: 'syncType must be "full" or "incremental"' });
      }

      // Check connector is configured before publishing
      const raw = await deps.configStore.get(etcdConfigKey(orgId));
      if (!raw) {
        return res
          .status(400)
          .json({ error: 'InnoVint connector is not configured for this org' });
      }

      await publishResyncEvent(deps.kafkaProducer, orgId, syncType as 'full' | 'incremental');

      return res.json({
        success:  true,
        message:  `${syncType} sync triggered`,
        syncType,
      });
    } catch (err) {
      console.error('[InnoVint] POST /sync error:', err);
      return res.status(500).json({ error: 'Failed to trigger sync' });
    }
  });

  // ── GET /status ──────────────────────────────────────────────────────────
  // Return whether the connector is configured for this org.
  router.get('/status', async (req: Request, res: Response) => {
    try {
      const orgId: string = (req as any).orgId;
      const raw = await deps.configStore.get(etcdConfigKey(orgId));

      if (!raw) {
        const status: InnovintConnectorStatus = {
          connectorType:   'innovint',
          configured:      false,
          enabledEntities: [],
        };
        return res.json(status);
      }

      const config: InnovintConfig = JSON.parse(raw);
      const status: InnovintConnectorStatus = {
        connectorType:   'innovint',
        configured:      true,
        enabledEntities: config.enabledEntities,
      };
      return res.json(status);
    } catch (err) {
      console.error('[InnoVint] GET /status error:', err);
      return res.status(500).json({ error: 'Failed to fetch status' });
    }
  });

  // ── DELETE /config ───────────────────────────────────────────────────────
  // Remove connector configuration from ETCD.
  router.delete('/config', async (req: Request, res: Response) => {
    try {
      const orgId: string = (req as any).orgId;
      await deps.configStore.delete(etcdConfigKey(orgId));

      return res.json({ success: true, message: 'InnoVint connector removed' });
    } catch (err) {
      console.error('[InnoVint] DELETE /config error:', err);
      return res.status(500).json({ error: 'Failed to remove configuration' });
    }
  });

  return router;
}
