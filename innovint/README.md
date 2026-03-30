# InnoVint Connector for PipesHub AI

Syncs winery production data from [InnoVint](https://www.innovint.us) into PipesHub AI for enterprise search and AI-powered Q&A.

---

## What It Syncs

| Entity | Description |
|--------|-------------|
| Lots | Wine lot records — vintage, varietal, appellation, vineyard, volume, status, tags |
| Vessels | Tanks, barrels, bins, kegs — location, capacity, cooperage, contained lot |
| Analyses | Lab results — pH, TA, VA, SO₂, alcohol, Brix, temperature |
| Work Orders | Cellar tasks — title, type, status, assignee, priority, due date |
| Case Goods | Finished wine inventory — SKU, product name, cases on hand, location |
| Costs | Lot cost entries — category, amount, vendor, unit cost |

---

## Prerequisites

- PipesHub AI instance running ([setup guide](https://github.com/pipeshub-ai/pipeshub-ai))
- InnoVint account with API access enabled
- InnoVint API key and tenant name

---

## Getting Your InnoVint Credentials

1. Log in to InnoVint and go to **Settings → Integrations**
2. If an API key is shown, copy it directly. Otherwise, click **Request API Access** or email `support@innovint.us`
3. Your **tenant name** is the subdomain of your InnoVint URL — e.g. `my-winery` from `my-winery.innovint.us`
4. Verify access with a quick test:
   ```bash
   curl -H "X-API-Key: YOUR_API_KEY" \
        https://YOUR_TENANT.innovint.us/api/v1/lots | head -c 200
   ```

---

## Installation

The fastest way is to use the included `deploy.sh` script:

```bash
./deploy.sh /path/to/pipeshub-ai
```

This copies both Python packages and the Node.js module into the right locations. Then follow the two manual steps it prints — applying `connector_factory.patch.md` and registering the router.

### Manual installation

1. Copy the Python packages into your PipesHub AI installation:
   ```bash
   cp -r backend/python/app/sources/external/innovint/ \
         /path/to/pipeshub-ai/backend/python/app/sources/external/innovint/

   cp -r backend/python/app/connectors/sources/innovint/ \
         /path/to/pipeshub-ai/backend/python/app/connectors/sources/innovint/
   ```

2. Install the Python dependency:
   ```bash
   pip install aiohttp
   ```

3. Register the connector in `connector_factory.py` (see `backend/python/app/connectors/core/factory/connector_factory.patch.md` for the exact diff):
   ```python
   from app.connectors.sources.innovint.connector import InnovintConnector

   # Add to _CONNECTOR_MAP:
   "innovint": InnovintConnector,
   ```

4. Copy the Node.js Connector Manager module:
   ```bash
   cp -r services/nodejs/apps/connector-manager/src/modules/innovint/ \
         /path/to/pipeshub-ai/services/nodejs/apps/connector-manager/src/modules/innovint/
   ```

5. Register the router in your connector manager (usually `connectors.router.ts`):
   ```typescript
   import { innovintRouter } from './modules/innovint/innovint.connector';
   app.use('/api/v1/connectors/innovint', innovintRouter);
   ```
   Then rebuild: `cd /path/to/pipeshub-ai/services/nodejs && npm run build`

6. Rebuild and restart your Docker stack:
   ```bash
   docker compose -f deployment/docker-compose/docker-compose.dev.yml build --no-cache pipeshub-ai
   docker compose -f deployment/docker-compose/docker-compose.dev.yml up -d
   ```

7. Save your credentials via the API to start the first sync:
   ```bash
   curl -X POST http://<pipeshub-host>/api/v1/connectors/innovint/config \
        -H "Authorization: Bearer YOUR_PIPESHUB_TOKEN" \
        -H "Content-Type: application/json" \
        -d '{"tenant":"my-winery","authType":"api_key","apiKey":"YOUR_API_KEY"}'
   ```

---

## Configuration

| Field | Description |
|-------|-------------|
| tenant | Your InnoVint subdomain (e.g. `my-winery`) |
| authType | `api_key` or `bearer` |
| apiKey | Your InnoVint API key (stored encrypted in ETCD) |

---

## Architecture

The connector follows PipesHub AI's ConnectorFactory playbook:

- **Inbound trigger:** `innovint.resync` event on the `sync-events` Kafka topic
- **Credentials:** stored in ETCD at `services/connectors/innovint/{orgId}/config` — never in Kafka messages
- **Sync modes:** full (all records) and incremental (updated since last SyncPoint)
- **Record types:** `WebpageRecord` for Lots/Vessels/Analyses/Case Goods/Costs; `TicketRecord` for Work Orders
- **Permissions:** READER for all active org users; OWNER for admins
- **On 401:** automatically re-reads ETCD and retries once before aborting

```
InnoVint API → InnovintClient → InnovintConnector → ArangoDB Graph DB → record-events → Indexer → stream_record()
```

For full architecture details, see [`docs/Innovint-PipesHub-Connector-Documentation.docx`](./docs/Innovint-PipesHub-Connector-Documentation.docx).

---

## File Structure

```
innovint/
├── README.md                                          # This file
├── deploy.sh                                          # One-command installer
├── requirements.txt                                   # Python deps (aiohttp only)
├── backend/python/app/
│   ├── sources/external/innovint/innovint.py          # Async HTTP client (InnovintClient)
│   └── connectors/sources/innovint/
│       ├── connector.py                               # InnovintConnector(BaseConnector)
│       ├── common/apps.py                             # App name / group constants
│       ├── test.py                                    # 49-test suite
│       └── ../core/factory/connector_factory.patch.md # ConnectorFactory registration diff
├── services/nodejs/.../innovint/innovint.connector.ts # Node.js Connector Manager module
├── config/innovint.env.example                        # Environment variable template
├── docker/Dockerfile                                  # Dev/CI Dockerfile
└── docs/                                              # Full documentation
    ├── Innovint-PipesHub-Connector-Documentation.docx
    └── Innovint-PipesHub-Setup-and-Usage-Guide.docx
```

---

## Documentation

Detailed technical documentation and a step-by-step setup guide are in the [`docs/`](./docs/) folder.

---

## License

Apache 2.0 — see [LICENSE](../LICENSE).
