"""
InnoVint Connector — PipesHub Integration
==========================================
Syncs winery production data from InnoVint into PipesHub's Graph DB.

Follows the PipesHub Connector Integration Playbook:
  - Extends BaseConnector
  - Registered via @ConnectorBuilder decorator
  - Reads credentials from ETCD via ConfigurationService
  - Persists records + permissions to Graph DB via DataSourceEntitiesProcessor
  - Publishes record-events for the indexing pipeline
  - Supports delta/incremental sync via SyncPoint timestamps
  - Implements stream_record() for on-demand content retrieval

Entity → RecordType mapping:
  Lots, Vessels, Analysis, Case Goods, Costs  → RecordType.WEBPAGE
  Work Orders                                  → RecordType.TICKET

Usage (triggered by sync-events Kafka topic, event: innovint.resync):
  The ConnectorFactory instantiates InnovintConnector when it receives
  an innovint.resync event. No manual wiring needed once registered.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from logging import Logger
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from app.config.configuration_service import ConfigurationService
from app.config.constants.arangodb import (
    CollectionNames,
    Connectors,
    OriginTypes,
    RecordRelations,
)
from app.connectors.core.base.connector.connector_service import BaseConnector
from app.connectors.core.base.data_processor.data_source_entities_processor import (
    DataSourceEntitiesProcessor,
)
from app.connectors.core.base.data_store.data_store import DataStoreProvider
from app.connectors.core.base.sync_point.sync_point import (
    SyncDataPointType,
    SyncPoint,
    generate_record_sync_point_key,
)
from app.connectors.core.registry.connector_builder import (
    AuthField,
    ConnectorBuilder,
    DocumentationLink,
    FilterField,
)
from app.connectors.sources.innovint.common.apps import InnovintApp
from app.models.entities import (
    AppUser,
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
    TicketRecord,
    WebpageRecord,
)
from app.models.permission import EntityType, Permission, PermissionType

# External API client (moved to sources/external/ per playbook structure)
from app.sources.external.innovint.innovint import (
    InnovintAPIError,
    InnovintAuthError,
    InnovintClient as InnovintAPIClient,
)


# ── Entity types synced by this connector ────────────────────────────────
ENTITY_TYPES = ["lots", "vessels", "analyses", "work_orders", "case_goods", "costs"]

# ── Record type per entity ───────────────────────────────────────────────
ENTITY_RECORD_TYPE: Dict[str, RecordType] = {
    "lots":        RecordType.WEBPAGE,
    "vessels":     RecordType.WEBPAGE,
    "analyses":    RecordType.WEBPAGE,
    "work_orders": RecordType.TICKET,
    "case_goods":  RecordType.WEBPAGE,
    "costs":       RecordType.WEBPAGE,
}

# ── Human-readable group names ───────────────────────────────────────────
ENTITY_GROUP_NAMES: Dict[str, str] = {
    "lots":        "InnoVint Lots",
    "vessels":     "InnoVint Vessels",
    "analyses":    "InnoVint Lab Analysis",
    "work_orders": "InnoVint Work Orders",
    "case_goods":  "InnoVint Case Goods",
    "costs":       "InnoVint Cost Entries",
}


# ═══════════════════════════════════════════════════════════════════════════
# Connector class
# ═══════════════════════════════════════════════════════════════════════════

@ConnectorBuilder("InnoVint") \
    .in_group("InnoVint") \
    .with_auth_type("API_TOKEN") \
    .with_description("Sync winery production data (lots, vessels, lab analysis, work orders, inventory, costs) from InnoVint into PipesHub for AI-powered search and workflows.") \
    .with_categories(["Winery Management", "Production", "Inventory"]) \
    .configure(lambda builder: builder
        .with_icon("/assets/icons/connectors/innovint.svg")
        .add_documentation_link(DocumentationLink(
            "InnoVint API Docs",
            "https://www.innovint.us/api-documentation"
        ))
        .add_documentation_link(DocumentationLink(
            "Setup Guide",
            "https://docs.pipeshub.com/connectors/innovint"
        ))
        .add_auth_field(AuthField(
            name="apiKey",
            display_name="API Key",
            placeholder="Enter your InnoVint API key",
            description="API key from your InnoVint account settings (Settings → API Access)",
            field_type="PASSWORD",
            is_secret=True,
        ))
        .add_auth_field(AuthField(
            name="tenant",
            display_name="Winery Subdomain",
            placeholder="my-winery",
            description="Your InnoVint subdomain — the part before '.innovint.us'",
        ))
        .add_filter_field(FilterField(
            name="entityTypes",
            display_name="Data Types to Sync",
            description="Select which InnoVint data types to include",
            field_type="MULTISELECT",
        ), "static")
        .with_sync_strategies(["SCHEDULED", "MANUAL"])
        .with_scheduled_config(True, 60)   # default: every 60 minutes
    ) \
    .build_decorator()
class InnovintConnector(BaseConnector):
    """
    Connector that syncs InnoVint winery data into PipesHub.

    Data flow:
      sync-events (Kafka) → ConnectorFactory → InnovintConnector.init()
        → InnovintAPIClient (InnoVint REST API)
        → transform to Record + Permission objects
        → DataSourceEntitiesProcessor.on_new_records() → Graph DB
        → record-events (Kafka) → PipesHub indexing pipeline
        → stream_record() called by indexer to get content
    """

    def __init__(
        self,
        logger: Logger,
        data_entities_processor: DataSourceEntitiesProcessor,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
    ) -> None:
        super().__init__(
            InnovintApp(),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
        )

        self.api_client: Optional[InnovintAPIClient] = None
        self.tenant: Optional[str] = None

        # One SyncPoint per entity type for delta/incremental sync.
        # Stores: {'last_sync_timestamp': epoch_ms}
        self._sync_points: Dict[str, SyncPoint] = {
            entity_type: SyncPoint(
                connector_name=Connectors.INNOVINT,
                org_id=self.data_entities_processor.org_id,
                sync_data_point_type=SyncDataPointType.RECORDS,
                data_store_provider=self.data_store_provider,
            )
            for entity_type in ENTITY_TYPES
        }

        # Cache of RecordGroup IDs per entity type, populated on first sync
        self._record_group_ids: Dict[str, str] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def init(self) -> bool:
        """
        Load credentials from ETCD via ConfigurationService and
        initialize the InnoVint API client.
        """
        try:
            org_id = self.data_entities_processor.org_id
            credentials = await self._get_credentials(org_id)

            self.tenant = credentials["tenant"]
            self.api_client = InnovintAPIClient(
                tenant=self.tenant,
                api_key=credentials["apiKey"],
            )

            if not await self.test_connection_and_access():
                self.logger.error("InnoVint connection test failed — check API key and tenant")
                return False

            self.logger.info(
                f"InnovintConnector initialized for org={org_id}, tenant={self.tenant}"
            )
            return True

        except Exception as e:
            self.logger.error(f"InnovintConnector.init() failed: {e}", exc_info=True)
            return False

    async def test_connection_and_access(self) -> bool:
        """Verify the API key is valid by making a lightweight API call."""
        try:
            async with self.api_client:
                return await self.api_client.health_check()
        except Exception as e:
            self.logger.error(f"InnoVint connection test failed: {e}")
            return False

    async def _get_credentials(self, org_id: str) -> Dict[str, str]:
        """
        Load InnoVint credentials from ETCD via ConfigurationService.

        Config is stored at: /services/connectors/innovint/config
        Shape: { "auth": { "apiKey": "...", "tenant": "my-winery" } }
        """
        config_path = "/services/connectors/innovint/config"
        config = await self.config_service.get_config(config_path)

        if not config:
            raise ValueError(
                f"InnoVint connector config not found at {config_path}. "
                "Configure the connector in PipesHub settings first."
            )

        auth = config.get("auth", {})
        api_key = auth.get("apiKey")
        tenant  = auth.get("tenant")

        if not api_key or not tenant:
            raise ValueError(
                "InnoVint config is missing 'apiKey' or 'tenant' in auth block."
            )

        return {"apiKey": api_key, "tenant": tenant}

    async def cleanup(self) -> None:
        """Release the aiohttp session."""
        try:
            if self.api_client:
                await self.api_client.close()
                self.api_client = None
        except Exception as e:
            self.logger.error(f"InnovintConnector.cleanup() error: {e}")

    # ── Sync ──────────────────────────────────────────────────────────────

    async def run_sync(self) -> None:
        """
        Full sync — fetch all records from every enabled entity type
        and upsert them into PipesHub's Graph DB.

        Triggered when: syncAction=immediate or a manual sync is requested.

        SyncPoint timing: we capture sync_start_ts ONCE here, before any
        entity is fetched.  All entity SyncPoints are saved to this same
        timestamp so that records updated while an earlier entity was being
        fetched are guaranteed to appear in the next incremental run.
        """
        org_id = self.data_entities_processor.org_id
        self.logger.info(f"InnoVint full sync starting for org={org_id}, tenant={self.tenant}")

        # Capture the sync start time before ANY fetching begins.
        # Using this single timestamp for all entity SyncPoints prevents
        # a race where a record updated during the sync is silently missed
        # by the next incremental run.
        sync_start_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Ensure RecordGroups exist in Graph DB for each entity type
        await self._ensure_record_groups(org_id)

        # Get all active PipesHub users — InnoVint data is org-wide,
        # so all active users receive READER permission on every record.
        active_users = await self.data_entities_processor.get_all_active_users()

        async with self.api_client:
            for entity_type in ENTITY_TYPES:
                try:
                    count = await self._sync_entity_type(
                        org_id, entity_type, active_users,
                        updated_since=None,
                        sync_start_ts=sync_start_ts,
                    )
                    self.logger.info(f"  {entity_type}: {count} records synced")
                except InnovintAuthError:
                    self.logger.warning(
                        f"401 on {entity_type} — API key may have been rotated; "
                        "reloading credentials from ETCD"
                    )
                    if await self._reload_credentials():
                        try:
                            count = await self._sync_entity_type(
                                org_id, entity_type, active_users,
                                updated_since=None,
                                sync_start_ts=sync_start_ts,
                            )
                            self.logger.info(
                                f"  {entity_type}: {count} records synced (after credential reload)"
                            )
                        except InnovintAuthError:
                            self.logger.error(
                                "Credentials still invalid after reload — aborting sync. "
                                "Update the API key in PipesHub connector settings."
                            )
                            return
                    else:
                        self.logger.error("Credential reload failed — aborting sync.")
                        return
                except Exception as e:
                    self.logger.error(
                        f"Error syncing {entity_type} for org={org_id}: {e}",
                        exc_info=True,
                    )
                    # Continue with remaining entity types — don't abort the whole sync

        self.logger.info(f"InnoVint full sync complete for org={org_id}")

    async def run_incremental_sync(self) -> None:
        """
        Incremental sync — only fetch records updated since the last
        successful sync for each entity type.

        Uses per-entity-type SyncPoints storing the last sync timestamp.

        SyncPoint timing: sync_start_ts is captured once here, before any
        entity is fetched, for the same reason as in run_sync().
        """
        org_id = self.data_entities_processor.org_id
        self.logger.info(f"InnoVint incremental sync starting for org={org_id}")

        # Capture before fetching — see run_sync() docstring for rationale.
        sync_start_ts = int(datetime.now(timezone.utc).timestamp() * 1000)

        await self._ensure_record_groups(org_id)
        active_users = await self.data_entities_processor.get_all_active_users()

        async with self.api_client:
            for entity_type in ENTITY_TYPES:
                try:
                    # Read last sync timestamp from SyncPoint store
                    sp_key = self._sync_point_key(entity_type, org_id)
                    sp_data = await self._sync_points[entity_type].read_sync_point(sp_key)
                    last_ts: Optional[int] = sp_data.get("last_sync_timestamp") if sp_data else None
                    updated_since: Optional[datetime] = (
                        datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
                        if last_ts else None
                    )

                    count = await self._sync_entity_type(
                        org_id, entity_type, active_users,
                        updated_since=updated_since,
                        sync_start_ts=sync_start_ts,
                    )
                    self.logger.info(
                        f"  {entity_type}: {count} records synced"
                        + (f" (since {updated_since.isoformat()})" if updated_since else " (first run)")
                    )

                except InnovintAuthError:
                    self.logger.warning(
                        f"401 on {entity_type} during incremental sync — "
                        "reloading credentials from ETCD"
                    )
                    if await self._reload_credentials():
                        try:
                            count = await self._sync_entity_type(
                                org_id, entity_type, active_users,
                                updated_since=updated_since,
                                sync_start_ts=sync_start_ts,
                            )
                            self.logger.info(
                                f"  {entity_type}: {count} records synced (after credential reload)"
                            )
                        except InnovintAuthError:
                            self.logger.error(
                                "Credentials still invalid after reload — aborting incremental sync."
                            )
                            return
                    else:
                        self.logger.error("Credential reload failed — aborting incremental sync.")
                        return
                except Exception as e:
                    self.logger.error(
                        f"Error in incremental sync of {entity_type}: {e}",
                        exc_info=True,
                    )

        self.logger.info(f"InnoVint incremental sync complete for org={org_id}")

    # ── Content streaming (called by PipesHub indexing pipeline) ──────────

    async def stream_record(self, record: Record) -> StreamingResponse:
        """
        Stream the textual content of an InnoVint record back to the indexer.

        Called by PipesHub's indexing pipeline after receiving a record-event.
        The external_record_id encodes both entity type and InnoVint ID:
            format: "{entity_type}:{innovint_id}"
            example: "lot:lot-001", "work_order:wo-007"
        """
        try:
            external_id = record.external_record_id or ""
            if ":" not in external_id:
                raise ValueError(
                    f"Unexpected external_record_id format: '{external_id}'. "
                    "Expected '{entity_type}:{innovint_id}'."
                )

            entity_type, innovint_id = external_id.split(":", 1)

            async with self.api_client:
                content = await self._fetch_record_content(entity_type, innovint_id)

            async def generate():
                yield content.encode("utf-8")

            return StreamingResponse(generate(), media_type="text/plain; charset=utf-8")

        except Exception as e:
            self.logger.error(
                f"stream_record failed for record {record.external_record_id}: {e}"
            )
            raise HTTPException(
                status_code=500,
                detail=f"Failed to stream InnoVint record content: {e}",
            )

    def get_signed_url(self, record: Record) -> Optional[str]:
        """
        Return the InnoVint web URL for direct browser navigation.
        Stored in record.weburl during sync — no extra API call needed.
        """
        return record.weburl or None

    # ── Private: sync orchestration ───────────────────────────────────────

    async def _reload_credentials(self) -> bool:
        """
        Re-read the API key from ETCD and rebuild the InnoVint client.

        Called when a 401 is received mid-sync — the admin may have rotated
        the key via the Connector Manager UI since init() ran.

        Returns True if the new client was created successfully, False otherwise.
        """
        try:
            org_id = self.data_entities_processor.org_id
            self.logger.info(f"Reloading InnoVint credentials from ETCD for org={org_id}")
            credentials = await self._get_credentials(org_id)

            # Close the stale client before replacing it
            if self.api_client:
                await self.api_client.close()

            self.tenant = credentials["tenant"]
            self.api_client = InnovintAPIClient(
                tenant=self.tenant,
                api_key=credentials["apiKey"],
            )
            self.logger.info("InnoVint credentials reloaded successfully.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to reload InnoVint credentials: {e}", exc_info=True)
            return False

    async def _sync_entity_type(
        self,
        org_id: str,
        entity_type: str,
        active_users: List[AppUser],
        updated_since: Optional[datetime],
        sync_start_ts: int,
    ) -> int:
        """
        Fetch one entity type from InnoVint, transform to Records + Permissions,
        persist to Graph DB, and update the SyncPoint.

        sync_start_ts must be provided by the caller (run_sync /
        run_incremental_sync), captured ONCE before any entity is fetched.
        This prevents clock-skew data gaps when a long sync overlaps with
        InnoVint record updates.
        """
        fetcher = self._get_fetcher(entity_type)
        records_with_permissions: List[Tuple[Record, List[Permission]]] = []
        count = 0

        async for raw_item in fetcher(updated_since=updated_since):
            try:
                record, permissions = self._transform_to_record(
                    org_id, entity_type, raw_item, active_users
                )
                records_with_permissions.append((record, permissions))

                # Batch upsert every 50 records
                if len(records_with_permissions) >= 50:
                    await self.data_entities_processor.on_new_records(records_with_permissions)
                    count += len(records_with_permissions)
                    records_with_permissions = []

            except Exception as e:
                self.logger.warning(
                    f"Error transforming {entity_type} item "
                    f"id={raw_item.get('id', '?')}: {e}"
                )

        # Flush remaining records
        if records_with_permissions:
            await self.data_entities_processor.on_new_records(records_with_permissions)
            count += len(records_with_permissions)

        # Update SyncPoint so next incremental sync starts from now
        if count > 0:
            sp_key = self._sync_point_key(entity_type, org_id)
            await self._sync_points[entity_type].update_sync_point(
                sp_key,
                {"last_sync_timestamp": sync_start_ts},
            )

        return count

    def _get_fetcher(self, entity_type: str):
        """Return the api_client async generator method for an entity type."""
        fetcher_map = {
            "lots":        self.api_client.list_lots,
            "vessels":     self.api_client.list_vessels,
            "analyses":    self.api_client.list_analyses,
            "work_orders": self.api_client.list_work_orders,
            "case_goods":  self.api_client.list_case_goods,
            "costs":       self.api_client.list_costs,
        }
        fetcher = fetcher_map.get(entity_type)
        if not fetcher:
            raise ValueError(f"Unknown entity type: {entity_type}")
        return fetcher

    # ── Private: record transformation ───────────────────────────────────

    def _transform_to_record(
        self,
        org_id: str,
        entity_type: str,
        raw: Dict[str, Any],
        active_users: List[AppUser],
    ) -> Tuple[Record, List[Permission]]:
        """
        Convert a raw InnoVint API dict to the correct PipesHub Record subtype.

        The external_record_id encodes entity type + InnoVint ID so that
        stream_record() can reconstruct the right API fetch later.
        """
        now_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
        innovint_id = str(raw.get("id", raw.get("lot_id", raw.get("analysis_id", uuid.uuid4()))))
        record_id   = str(uuid.uuid4())
        base_url    = f"https://{self.tenant}.innovint.us"

        # external_record_id encodes both type and ID for stream_record()
        external_id = f"{entity_type.rstrip('s')}:{innovint_id}"

        if entity_type == "work_orders":
            record = self._make_ticket_record(
                record_id, org_id, raw, innovint_id, external_id, base_url, now_ms
            )
        else:
            record = self._make_webpage_record(
                record_id, org_id, entity_type, raw, innovint_id, external_id, base_url, now_ms
            )

        # Link to the correct RecordGroup
        record.external_record_group_id = self._record_group_ids.get(entity_type, "")

        permissions = self._build_permissions(active_users)
        return record, permissions

    def _make_webpage_record(
        self,
        record_id: str,
        org_id: str,
        entity_type: str,
        raw: Dict[str, Any],
        innovint_id: str,
        external_id: str,
        base_url: str,
        now_ms: int,
    ) -> WebpageRecord:
        """Build a WebpageRecord for lots, vessels, analyses, case goods, costs."""
        url_map = {
            "lots":        f"{base_url}/lots/{innovint_id}",
            "vessels":     f"{base_url}/vessels/{innovint_id}",
            "analyses":    f"{base_url}/analyses/{innovint_id}",
            "case_goods":  f"{base_url}/case-goods/{innovint_id}",
            "costs":       f"{base_url}/costs/{innovint_id}",
        }
        record_name = self._derive_name(entity_type, raw)

        source_created = self._parse_epoch_ms(
            raw.get("created_at") or raw.get("analysis_date") or raw.get("cost_date")
        )
        source_updated = self._parse_epoch_ms(
            raw.get("updated_at") or raw.get("modified_at")
        )

        return WebpageRecord(
            id=record_id,
            org_id=org_id,
            record_name=record_name,
            record_type=RecordType.WEBPAGE,
            external_record_id=external_id,
            connector_name=Connectors.INNOVINT,
            origin=OriginTypes.CONNECTOR,
            version=0,
            weburl=url_map.get(entity_type, base_url),
            created_at=now_ms,
            updated_at=now_ms,
            source_created_at=source_created,
            source_updated_at=source_updated,
        )

    def _make_ticket_record(
        self,
        record_id: str,
        org_id: str,
        raw: Dict[str, Any],
        innovint_id: str,
        external_id: str,
        base_url: str,
        now_ms: int,
    ) -> TicketRecord:
        """Build a TicketRecord for work orders."""
        # Map InnoVint statuses to PipesHub ticket status values
        status_map = {
            "pending":     "OPEN",
            "in_progress": "IN_PROGRESS",
            "completed":   "CLOSED",
            "cancelled":   "CANCELLED",
        }
        raw_status = str(raw.get("status", "pending")).lower()

        source_created = self._parse_epoch_ms(raw.get("created_at"))
        source_updated = self._parse_epoch_ms(raw.get("updated_at"))
        due_date_ms    = self._parse_epoch_ms(raw.get("due_date"))

        return TicketRecord(
            id=record_id,
            org_id=org_id,
            record_name=raw.get("title", raw.get("name", f"Work Order {innovint_id}")),
            record_type=RecordType.TICKET,
            external_record_id=external_id,
            connector_name=Connectors.INNOVINT,
            origin=OriginTypes.CONNECTOR,
            version=0,
            weburl=f"{base_url}/work-orders/{innovint_id}",
            created_at=now_ms,
            updated_at=now_ms,
            source_created_at=source_created,
            source_updated_at=source_updated,
            status=status_map.get(raw_status, "OPEN"),
            priority=str(raw.get("priority", "normal")).upper(),
            assignee=raw.get("assigned_to") or raw.get("assignee"),
            due_date=due_date_ms,
        )

    def _build_permissions(self, active_users: List[AppUser]) -> List[Permission]:
        """
        Grant READER access to all active org users.

        InnoVint data is org-wide — all users in the PipesHub org can
        search and read it. Owners/admins are mapped to OWNER permission.
        """
        permissions = []
        for user in active_users:
            role = PermissionType.OWNER if getattr(user, "is_admin", False) else PermissionType.READ
            permissions.append(
                Permission(
                    email=user.email,
                    type=role,
                    entity_type=EntityType.USER,
                )
            )
        return permissions

    # ── Private: content fetching (for stream_record) ─────────────────────

    async def _fetch_record_content(self, entity_type: str, innovint_id: str) -> str:
        """
        Re-fetch a single entity from InnoVint and return its full-text content.

        The content string is what PipesHub's indexer embeds and makes searchable.
        """
        # Map entity_type (singular, e.g. "lot") back to API fetcher
        type_to_fetcher = {
            "lot":        self.api_client.get_lot,
            "vessel":     self.api_client.get_vessel,
            "analysis":   self.api_client.get_analysis,
            "work_order": self.api_client.get_work_order,
            "case_good":  self.api_client.get_case_good,
            "cost":       self.api_client.get_cost,
        }
        fetcher = type_to_fetcher.get(entity_type)
        if not fetcher:
            raise ValueError(f"Unknown entity type for content streaming: '{entity_type}'")

        raw = await fetcher(innovint_id)
        return self._raw_to_text(entity_type, raw)

    def _raw_to_text(self, entity_type: str, raw: Dict[str, Any]) -> str:
        """Convert raw API dict to full-text content string for indexing."""
        lines = []

        if entity_type == "lot":
            lines = [
                f"Wine Lot: {raw.get('lot_name', '')} ({raw.get('lot_code', '')})",
                f"Vintage: {raw.get('vintage', '')}" if raw.get("vintage") else None,
                f"Varietal: {raw.get('varietal') or raw.get('variety', '')}" or None,
                f"Appellation: {raw.get('appellation', '')}" if raw.get("appellation") else None,
                f"Vineyard: {raw.get('vineyard') or raw.get('vineyard_name', '')}" or None,
                f"Status: {raw.get('status', '')}",
                f"Volume: {raw.get('volume_gallons', '')} gal" if raw.get("volume_gallons") else None,
                f"Color: {raw.get('color', '')}" if raw.get("color") else None,
                f"Brand: {raw.get('brand', '')}" if raw.get("brand") else None,
                raw.get("description") or raw.get("notes"),
            ]
        elif entity_type == "vessel":
            lines = [
                f"Vessel: {raw.get('name', '')} ({raw.get('type') or raw.get('vessel_type', '')})",
                f"Location: {raw.get('location', '')}" if raw.get("location") else None,
                f"Building: {raw.get('building', '')}" if raw.get("building") else None,
                f"Capacity: {raw.get('capacity_gallons', '')} gal" if raw.get("capacity_gallons") else None,
                f"Contains Lot: {raw.get('lot_code', '')}" if raw.get("lot_code") else None,
                f"Material: {raw.get('material', '')}" if raw.get("material") else None,
                f"Cooperage: {raw.get('cooperage', '')}" if raw.get("cooperage") else None,
                f"Status: {raw.get('status', '')}" if raw.get("status") else None,
            ]
        elif entity_type == "analysis":
            lines = [
                f"Lab Analysis for Lot: {raw.get('lot_code') or raw.get('lot_id', '')}",
                f"Date: {raw.get('analysis_date') or raw.get('date', '')}" or None,
                f"Type: {raw.get('analysis_type') or raw.get('type', '')}" or None,
                f"Lab: {raw.get('lab_name') or raw.get('lab', '')}" or None,
                f"pH: {raw.get('ph')}" if raw.get("ph") is not None else None,
                f"TA: {raw.get('titratable_acidity') or raw.get('ta')} g/L" if (raw.get('titratable_acidity') or raw.get('ta')) else None,
                f"Alcohol: {raw.get('alcohol') or raw.get('abv')}%" if (raw.get("alcohol") or raw.get("abv")) else None,
                f"Brix: {raw.get('brix')}" if raw.get("brix") is not None else None,
                raw.get("notes"),
            ]
        elif entity_type == "work_order":
            lines = [
                f"Work Order: {raw.get('title') or raw.get('name', '')}",
                f"Type: {raw.get('work_type') or raw.get('type', '')}" or None,
                f"Status: {raw.get('status', '')}",
                f"Priority: {raw.get('priority', '')}" if raw.get("priority") else None,
                f"Assigned To: {raw.get('assigned_to') or raw.get('assignee', '')}" or None,
                f"Lot: {raw.get('lot_code', '')}" if raw.get("lot_code") else None,
                f"Due: {raw.get('due_date', '')}" if raw.get("due_date") else None,
                raw.get("description"),
                raw.get("notes"),
            ]
        elif entity_type == "case_good":
            lines = [
                f"Case Good: {raw.get('product_name') or raw.get('name', '')}",
                f"SKU: {raw.get('sku', '')}" if raw.get("sku") else None,
                f"Brand: {raw.get('brand', '')}" if raw.get("brand") else None,
                f"Vintage: {raw.get('vintage', '')}" if raw.get("vintage") else None,
                f"Varietal: {raw.get('varietal') or raw.get('variety', '')}" or None,
                f"Size: {raw.get('bottle_size_ml', '')}mL" if raw.get("bottle_size_ml") else None,
                f"Cases on Hand: {raw.get('cases_on_hand') or raw.get('quantity', '')}" or None,
                f"Location: {raw.get('warehouse_location') or raw.get('location', '')}" or None,
            ]
        elif entity_type == "cost":
            lines = [
                f"Cost Entry for Lot: {raw.get('lot_code') or raw.get('lot_id', '')}",
                f"Category: {raw.get('category', '')}" if raw.get("category") else None,
                f"Amount: {raw.get('currency', 'USD')} {raw.get('amount', '')}",
                f"Vendor: {raw.get('vendor', '')}" if raw.get("vendor") else None,
                raw.get("description"),
            ]

        return "\n".join(line for line in lines if line)

    # ── Private: RecordGroup management ───────────────────────────────────

    async def _ensure_record_groups(self, org_id: str) -> None:
        """
        Create RecordGroups in Graph DB for each InnoVint entity type if
        they don't already exist, and cache their IDs for record linking.
        """
        for entity_type in ENTITY_TYPES:
            if entity_type in self._record_group_ids:
                continue  # already cached

            group_external_id = f"innovint_{self.tenant}_{entity_type}"
            group_name        = ENTITY_GROUP_NAMES[entity_type]

            record_group = RecordGroup(
                id=str(uuid.uuid4()),
                org_id=org_id,
                group_name=group_name,
                connector_name=Connectors.INNOVINT,
                external_group_id=group_external_id,
                record_group_type=RecordGroupType.FOLDER,
            )

            # on_new_record_group upserts by external_group_id (idempotent)
            saved_group = await self.data_entities_processor.on_new_record_group(record_group)
            self._record_group_ids[entity_type] = saved_group.id
            self.logger.debug(
                f"RecordGroup ready: {group_name} (id={saved_group.id})"
            )

    # ── Private: helpers ──────────────────────────────────────────────────

    @staticmethod
    def _sync_point_key(entity_type: str, org_id: str) -> str:
        """Generate a stable SyncPoint key for a given entity type + org."""
        return generate_record_sync_point_key(
            ENTITY_RECORD_TYPE.get(entity_type, RecordType.WEBPAGE).value,
            "org",
            org_id,
        ) + f"_{entity_type}"

    @staticmethod
    def _parse_epoch_ms(value: Any) -> Optional[int]:
        """Parse an ISO datetime string (or epoch) to epoch milliseconds."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value) if value > 1e10 else int(value * 1000)
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _derive_name(entity_type: str, raw: Dict[str, Any]) -> str:
        """Generate a human-readable record name from raw InnoVint data."""
        if entity_type == "lots":
            return f"Lot: {raw.get('lot_name', '')} ({raw.get('lot_code', raw.get('id', ''))})"
        if entity_type == "vessels":
            return f"Vessel: {raw.get('name', raw.get('id', ''))}"
        if entity_type == "analyses":
            lot = raw.get("lot_code") or raw.get("lot_id", "")
            date = (raw.get("analysis_date") or raw.get("date", ""))[:10]
            return f"Analysis: Lot {lot} ({date})"
        if entity_type == "case_goods":
            return f"Case Good: {raw.get('product_name') or raw.get('name', raw.get('id', ''))}"
        if entity_type == "costs":
            cat = raw.get("category", "General")
            lot = raw.get("lot_code") or raw.get("lot_id", "")
            return f"Cost: {cat} — Lot {lot}"
        return f"InnoVint {entity_type}: {raw.get('id', '')}"

    # ── Factory method ────────────────────────────────────────────────────

    @classmethod
    async def create_connector(
        cls,
        logger: Logger,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
    ) -> "InnovintConnector":
        """
        Factory method used by ConnectorFactory to instantiate this connector.
        Creates and initializes a DataSourceEntitiesProcessor, then returns a
        fully constructed InnovintConnector.
        """
        data_entities_processor = DataSourceEntitiesProcessor(
            logger,
            data_store_provider,
            config_service,
        )
        await data_entities_processor.initialize()

        return cls(
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
        )
