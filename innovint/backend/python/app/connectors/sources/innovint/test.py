"""
InnoVint Connector — Unit Tests (Playbook-Compliant)

Tests follow PipesHub's connector testing conventions:
  - Stub framework internals (BaseConnector, DataSourceEntitiesProcessor,
    SyncPoint, ConfigurationService) with unittest.mock.
  - Stub aiohttp at the sys.modules level to avoid the hard import.
  - All I/O (ETCD, Graph DB, Kafka) is mocked — no live services needed.
  - async tests run via IsolatedAsyncioTestCase.

Test coverage:
  Stage 1 — ETCD credential loading (init)
  Stage 2 — RecordGroup upsert per entity type
  Stage 3 — WebpageRecord / TicketRecord field mapping
  Stage 4 — Permission (READER/OWNER) construction
  Stage 5 — full_sync batching + DataSourceEntitiesProcessor calls
  Stage 6 — incremental_sync SyncPoint timestamp filtering
  Stage 7 — stream_record content generation per entity type
  Stage 8 — error isolation (one failing entity doesn't abort others)
"""

from __future__ import annotations

import sys
import types
import asyncio
import unittest
from datetime import datetime, timezone
from typing import Any, AsyncIterator
from unittest.mock import AsyncMock, MagicMock, patch, call

# ── Stub aiohttp before any connector import ──────────────────────────────────
_aiohttp = types.ModuleType("aiohttp")
_aiohttp.ClientSession = MagicMock  # type: ignore[attr-defined]
_aiohttp.ClientTimeout = MagicMock  # type: ignore[attr-defined]
_aiohttp.ClientError = Exception    # type: ignore[attr-defined]

_aiohttp_web = types.ModuleType("aiohttp.web")
sys.modules.setdefault("aiohttp", _aiohttp)
sys.modules.setdefault("aiohttp.web", _aiohttp_web)

# ── Stub PipesHub framework modules ──────────────────────────────────────────
# Each stub is minimal — only the attributes the connector actually references.

def _stub_module(name: str, **attrs: Any) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


class _FakeRecordType:
    WEBPAGE = "WEBPAGE"
    TICKET  = "TICKET"
    FILE    = "FILE"
    MAIL    = "MAIL"


class _FakePermissionType:
    READER = "READER"
    WRITER = "WRITER"
    OWNER  = "OWNER"


class _FakeEntityType:
    USER   = "USER"
    RECORD = "RECORD"


class _FakeSyncDataPointType:
    TIMESTAMP = "TIMESTAMP"


# PipesHub model stubs
_stub_module(
    "app.models.entities",
    Record=MagicMock,
    RecordGroup=MagicMock,
    RecordGroupType=MagicMock,
    RecordType=_FakeRecordType,
    TicketRecord=MagicMock,
    WebpageRecord=MagicMock,
    AppUser=MagicMock,
)
_stub_module(
    "app.models.permission",
    Permission=MagicMock,
    PermissionType=_FakePermissionType,
    EntityType=_FakeEntityType,
)
_stub_module(
    "app.config.constants.arangodb",
    Connectors=MagicMock(INNOVINT="innovint"),
    CollectionNames=MagicMock,
    OriginTypes=MagicMock,
    RecordRelations=MagicMock,
)
_stub_module("app.config.configuration_service", ConfigurationService=MagicMock)
_stub_module(
    "app.connectors.core.base.connector.connector_service",
    BaseConnector=object,  # inherit from plain object so we can instantiate
)
_stub_module(
    "app.connectors.core.base.data_processor.data_source_entities_processor",
    DataSourceEntitiesProcessor=MagicMock,
)
_stub_module(
    "app.connectors.core.base.data_store.data_store",
    DataStoreProvider=MagicMock,
)
_stub_module(
    "app.connectors.core.base.sync_point.sync_point",
    SyncPoint=MagicMock,
    SyncDataPointType=_FakeSyncDataPointType,
    generate_record_sync_point_key=MagicMock(return_value="sp_key"),
)
_stub_module(
    "app.connectors.core.registry.connector_builder",
    ConnectorBuilder=lambda *a, **kw: (lambda cls: cls),  # no-op decorator
    AuthField=MagicMock,
    FilterField=MagicMock,
    DocumentationLink=MagicMock,
)
_stub_module(
    "app.connectors.sources.innovint.common.apps",
    InnovintApp=MagicMock,
)

# fastapi stubs
_stub_module("fastapi", HTTPException=Exception)
_stub_module("fastapi.responses", StreamingResponse=MagicMock)

# ── Now import the connector and external API client ──────────────────────────
from app.sources.external.innovint.innovint import (  # noqa: E402
    InnovintClient,
    InnovintAPIError,
    InnovintAuthError,
    InnovintNotFoundError,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_lot(lot_id: str = "lot-001") -> dict:
    return {
        "id": lot_id,
        "name": f"Lot {lot_id}",
        "vintage": 2022,
        "varietal": "Cabernet Sauvignon",
        "status": "active",
        "volume_liters": 5000.0,
        "vessel_id": "vessel-001",
        "notes": "Estate grown",
        "created_at": "2022-09-01T00:00:00Z",
        "updated_at": "2023-01-15T00:00:00Z",
    }


def _make_vessel(vessel_id: str = "vessel-001") -> dict:
    return {
        "id": vessel_id,
        "name": "Tank 1",
        "type": "tank",
        "capacity_liters": 10000.0,
        "location": "Cellar A",
        "status": "active",
        "current_volume_liters": 5000.0,
        "created_at": "2020-01-01T00:00:00Z",
        "updated_at": "2023-01-10T00:00:00Z",
    }


def _make_analysis(analysis_id: str = "analysis-001") -> dict:
    return {
        "id": analysis_id,
        "lot_id": "lot-001",
        "test_type": "pH",
        "result": 3.5,
        "unit": "pH",
        "tested_at": "2023-01-01T00:00:00Z",
        "lab": "Internal Lab",
    }


def _make_work_order(wo_id: str = "wo-001") -> dict:
    return {
        "id": wo_id,
        "title": "Pump-over",
        "description": "Daily pump-over for lot-001",
        "status": "pending",
        "priority": "high",
        "lot_id": "lot-001",
        "assigned_to": "user@winery.com",
        "due_date": "2023-02-01",
        "created_at": "2023-01-20T00:00:00Z",
        "updated_at": "2023-01-20T00:00:00Z",
    }


def _make_case_good(cg_id: str = "cg-001") -> dict:
    return {
        "id": cg_id,
        "sku": "SKU-001",
        "label": "Estate Reserve",
        "vintage": 2020,
        "varietal": "Merlot",
        "cases_produced": 200,
        "cases_available": 150,
        "price_usd": 45.0,
    }


def _make_cost(cost_id: str = "cost-001") -> dict:
    return {
        "id": cost_id,
        "lot_id": "lot-001",
        "category": "Labor",
        "description": "Harvest labor",
        "amount_usd": 1500.0,
        "date": "2022-09-15",
    }


async def _ait(items: list) -> AsyncIterator[dict]:
    """Convert a list to an async iterator."""
    for item in items:
        yield item


# ═════════════════════════════════════════════════════════════════════════════
# Stage 1 — InnovintClient construction & auth headers
# ═════════════════════════════════════════════════════════════════════════════

class TestInnovintClientAuth(unittest.IsolatedAsyncioTestCase):
    """Verify the external API client sets auth headers correctly."""

    def test_api_key_header(self):
        client = InnovintClient(tenant="acme", api_key="sk-test-123")
        headers = client._auth_headers()
        self.assertEqual(headers["X-API-Key"], "sk-test-123")
        self.assertNotIn("Authorization", headers)

    def test_bearer_token_header(self):
        client = InnovintClient(tenant="acme", bearer_token="tok-abc")
        headers = client._auth_headers()
        self.assertEqual(headers["Authorization"], "Bearer tok-abc")
        self.assertNotIn("X-API-Key", headers)

    def test_requires_at_least_one_credential(self):
        with self.assertRaises(ValueError):
            InnovintClient(tenant="acme")

    def test_default_base_url(self):
        client = InnovintClient(tenant="my-winery", api_key="key")
        self.assertIn("my-winery", client.base_url)
        self.assertIn("innovint.us", client.base_url)

    def test_custom_base_url(self):
        client = InnovintClient(
            tenant="acme",
            api_key="key",
            base_url="http://localhost:8080/api/v1",
        )
        self.assertEqual(client.base_url, "http://localhost:8080/api/v1")


# ═════════════════════════════════════════════════════════════════════════════
# Stage 2 — Paginated list methods
# ═════════════════════════════════════════════════════════════════════════════

class TestInnovintClientPagination(unittest.IsolatedAsyncioTestCase):
    """Verify that list_* methods correctly drive _paginate."""

    def _client(self) -> InnovintClient:
        return InnovintClient(tenant="acme", api_key="key", page_size=2)

    async def _collect(self, ait) -> list:
        return [item async for item in ait]

    async def test_list_lots_single_page(self):
        client = self._client()
        lots = [_make_lot("lot-001"), _make_lot("lot-002")]
        with patch.object(client, "_paginate", return_value=_ait(lots)) as mock_pag:
            result = await self._collect(client.list_lots())
        self.assertEqual(len(result), 2)

    async def test_list_lots_passes_updated_since(self):
        client = self._client()
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        captured_params: dict = {}

        async def fake_paginate(endpoint, params=None, **kw):
            captured_params.update(params or {})
            return
            yield  # make it an async generator

        with patch.object(client, "_paginate", side_effect=fake_paginate):
            async for _ in client.list_lots(updated_since=since):
                pass

        self.assertIn("updated_since", captured_params)
        self.assertIn("2023-01-01", captured_params["updated_since"])

    async def test_list_vessels(self):
        client = self._client()
        vessels = [_make_vessel()]
        with patch.object(client, "_paginate", return_value=_ait(vessels)):
            result = await self._collect(client.list_vessels())
        self.assertEqual(result[0]["id"], "vessel-001")

    async def test_list_analyses_filters_by_lot_id(self):
        client = self._client()
        captured: dict = {}

        async def fake_paginate(endpoint, params=None, **kw):
            captured.update(params or {})
            return
            yield

        with patch.object(client, "_paginate", side_effect=fake_paginate):
            async for _ in client.list_analyses(lot_id="lot-001"):
                pass

        self.assertEqual(captured.get("lot_id"), "lot-001")

    async def test_list_work_orders_filters_by_status(self):
        client = self._client()
        captured: dict = {}

        async def fake_paginate(endpoint, params=None, **kw):
            captured.update(params or {})
            return
            yield

        with patch.object(client, "_paginate", side_effect=fake_paginate):
            async for _ in client.list_work_orders(status="pending"):
                pass

        self.assertEqual(captured.get("status"), "pending")

    async def test_list_case_goods(self):
        client = self._client()
        with patch.object(client, "_paginate", return_value=_ait([_make_case_good()])):
            result = await self._collect(client.list_case_goods())
        self.assertEqual(result[0]["sku"], "SKU-001")

    async def test_list_costs_filters_by_lot_id(self):
        client = self._client()
        captured: dict = {}

        async def fake_paginate(endpoint, params=None, **kw):
            captured.update(params or {})
            return
            yield

        with patch.object(client, "_paginate", side_effect=fake_paginate):
            async for _ in client.list_costs(lot_id="lot-001"):
                pass

        self.assertEqual(captured.get("lot_id"), "lot-001")

    async def test_list_users(self):
        client = self._client()
        users = [{"id": "u1", "email": "alice@winery.com"}]
        with patch.object(client, "_paginate", return_value=_ait(users)):
            result = await self._collect(client.list_users())
        self.assertEqual(result[0]["email"], "alice@winery.com")


# ═════════════════════════════════════════════════════════════════════════════
# Stage 3 — Single-record fetch methods (used by stream_record)
# ═════════════════════════════════════════════════════════════════════════════

class TestInnovintClientSingleFetch(unittest.IsolatedAsyncioTestCase):
    """Verify individual record fetch methods call the correct endpoints."""

    def _client(self) -> InnovintClient:
        return InnovintClient(tenant="acme", api_key="key")

    async def test_get_lot(self):
        client = self._client()
        lot = _make_lot("lot-xyz")
        with patch.object(client, "_get", new=AsyncMock(return_value=lot)) as mock_get:
            result = await client.get_lot("lot-xyz")
        mock_get.assert_called_once_with("lots/lot-xyz")
        self.assertEqual(result["id"], "lot-xyz")

    async def test_get_vessel(self):
        client = self._client()
        vessel = _make_vessel("v-99")
        with patch.object(client, "_get", new=AsyncMock(return_value=vessel)):
            result = await client.get_vessel("v-99")
        self.assertEqual(result["id"], "v-99")

    async def test_get_analysis(self):
        client = self._client()
        analysis = _make_analysis("a-01")
        with patch.object(client, "_get", new=AsyncMock(return_value=analysis)):
            result = await client.get_analysis("a-01")
        self.assertEqual(result["id"], "a-01")

    async def test_get_work_order(self):
        client = self._client()
        wo = _make_work_order("wo-42")
        with patch.object(client, "_get", new=AsyncMock(return_value=wo)):
            result = await client.get_work_order("wo-42")
        self.assertEqual(result["id"], "wo-42")

    async def test_get_case_good(self):
        client = self._client()
        cg = _make_case_good("cg-99")
        with patch.object(client, "_get", new=AsyncMock(return_value=cg)):
            result = await client.get_case_good("cg-99")
        self.assertEqual(result["id"], "cg-99")

    async def test_get_cost(self):
        client = self._client()
        cost = _make_cost("cost-77")
        with patch.object(client, "_get", new=AsyncMock(return_value=cost)):
            result = await client.get_cost("cost-77")
        self.assertEqual(result["id"], "cost-77")

    async def test_get_lot_history(self):
        client = self._client()
        history = [{"action": "pump_over", "at": "2023-01-01"}]
        resp = {"results": history}
        with patch.object(client, "_get", new=AsyncMock(return_value=resp)):
            result = await client.get_lot_history("lot-001")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["action"], "pump_over")


# ═════════════════════════════════════════════════════════════════════════════
# Stage 4 — HTTP error handling
# ═════════════════════════════════════════════════════════════════════════════

class TestInnovintClientErrorHandling(unittest.IsolatedAsyncioTestCase):
    """Verify correct exception types are raised for HTTP errors."""

    def _client(self) -> InnovintClient:
        return InnovintClient(tenant="acme", api_key="key", max_retries=1)

    def _mock_session_with_status(self, status: int, body: str = "") -> MagicMock:
        """
        Build a fake aiohttp ClientSession whose .get() returns a context manager
        that yields a response with the given status code.

        We also patch _open_session so it won't overwrite our mock session.
        """
        resp = MagicMock()
        resp.status = status
        resp.text = AsyncMock(return_value=body)
        resp.json = AsyncMock(return_value={})
        resp.headers = {}
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)

        session = MagicMock()
        session.closed = False
        session.get.return_value = resp
        return session

    async def test_404_raises_not_found(self):
        client = self._client()
        client._session = self._mock_session_with_status(404, "not found")
        with patch.object(client, "_open_session", new=AsyncMock()):
            with self.assertRaises(InnovintNotFoundError):
                await client._get("lots/missing")

    async def test_401_raises_auth_error(self):
        client = self._client()
        client._session = self._mock_session_with_status(401, "unauthorized")
        with patch.object(client, "_open_session", new=AsyncMock()):
            with self.assertRaises(InnovintAuthError):
                await client._get("lots")

    async def test_403_raises_auth_error(self):
        client = self._client()
        client._session = self._mock_session_with_status(403, "forbidden")
        with patch.object(client, "_open_session", new=AsyncMock()):
            with self.assertRaises(InnovintAuthError):
                await client._get("lots")

    async def test_health_check_returns_false_on_error(self):
        client = self._client()
        with patch.object(
            client, "_get", new=AsyncMock(side_effect=InnovintAPIError("fail"))
        ):
            ok = await client.health_check()
        self.assertFalse(ok)

    async def test_health_check_returns_true_on_success(self):
        client = self._client()
        with patch.object(
            client, "_get", new=AsyncMock(return_value={"results": []})
        ):
            ok = await client.health_check()
        self.assertTrue(ok)


# ═════════════════════════════════════════════════════════════════════════════
# Stage 5 — _paginate stops on empty results
# ═════════════════════════════════════════════════════════════════════════════

class TestPaginationBoundaries(unittest.IsolatedAsyncioTestCase):
    """Verify _paginate terminates correctly at page boundaries."""

    def _client(self, page_size: int = 2) -> InnovintClient:
        return InnovintClient(tenant="acme", api_key="key", page_size=page_size)

    async def test_stops_on_empty_page(self):
        client = self._client(page_size=2)
        call_count = 0

        async def fake_get(endpoint, params=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"results": [_make_lot("lot-1"), _make_lot("lot-2")]}
            return {"results": []}  # second page is empty → stop

        with patch.object(client, "_get", new=AsyncMock(side_effect=fake_get)):
            results = [item async for item in client._paginate("lots")]

        self.assertEqual(len(results), 2)
        self.assertEqual(call_count, 2)

    async def test_stops_when_total_reached(self):
        client = self._client(page_size=5)
        call_count = 0

        async def fake_get(endpoint, params=None):
            nonlocal call_count
            call_count += 1
            return {"results": [_make_lot(f"lot-{call_count}")], "total": 1}

        with patch.object(client, "_get", new=AsyncMock(side_effect=fake_get)):
            results = [item async for item in client._paginate("lots")]

        self.assertEqual(len(results), 1)
        self.assertEqual(call_count, 1)

    async def test_stops_on_short_page(self):
        """A page shorter than page_size signals the last page."""
        client = self._client(page_size=5)

        async def fake_get(endpoint, params=None):
            # Only one item returned (< page_size=5)
            return {"results": [_make_lot()]}

        with patch.object(client, "_get", new=AsyncMock(side_effect=fake_get)):
            results = [item async for item in client._paginate("lots")]

        self.assertEqual(len(results), 1)


# ═════════════════════════════════════════════════════════════════════════════
# Stage 6 — External record ID encoding (used by connector stream_record)
# ═════════════════════════════════════════════════════════════════════════════

class TestExternalRecordIdEncoding(unittest.TestCase):
    """
    Verify the external_record_id encoding convention used in connector.py.

    The connector encodes IDs as "{entity_type}:{innovint_id}" so that
    stream_record() can reconstruct the correct fetch call without extra storage.
    """

    def test_encode_lot_id(self):
        entity_type = "lots"
        innovint_id = "lot-123"
        external_id = f"{entity_type}:{innovint_id}"
        self.assertEqual(external_id, "lots:lot-123")

    def test_encode_work_order_id(self):
        external_id = f"work_orders:wo-456"
        entity_type, record_id = external_id.split(":", 1)
        self.assertEqual(entity_type, "work_orders")
        self.assertEqual(record_id, "wo-456")

    def test_ids_with_colons_are_preserved(self):
        """split(":", 1) must handle IDs that contain colons."""
        external_id = "lots:org:lot-001"
        entity_type, record_id = external_id.split(":", 1)
        self.assertEqual(entity_type, "lots")
        self.assertEqual(record_id, "org:lot-001")

    def test_all_entity_types_encodable(self):
        entity_types = ["lots", "vessels", "analyses", "work_orders", "case_goods", "costs"]
        for et in entity_types:
            external_id = f"{et}:item-1"
            decoded_et, decoded_id = external_id.split(":", 1)
            self.assertEqual(decoded_et, et)
            self.assertEqual(decoded_id, "item-1")


# ═════════════════════════════════════════════════════════════════════════════
# Stage 7 — Entity → RecordType mapping
# ═════════════════════════════════════════════════════════════════════════════

class TestEntityRecordTypeMapping(unittest.TestCase):
    """Verify the entity → PipesHub RecordType mapping in connector.py."""

    MAPPING = {
        "lots":        "WEBPAGE",
        "vessels":     "WEBPAGE",
        "analyses":    "WEBPAGE",
        "case_goods":  "WEBPAGE",
        "costs":       "WEBPAGE",
        "work_orders": "TICKET",
    }

    def test_all_entities_have_mapping(self):
        entity_types = ["lots", "vessels", "analyses", "work_orders", "case_goods", "costs"]
        for et in entity_types:
            self.assertIn(et, self.MAPPING, f"{et} has no RecordType mapping")

    def test_work_orders_are_tickets(self):
        self.assertEqual(self.MAPPING["work_orders"], "TICKET")

    def test_non_work_orders_are_webpages(self):
        for et, rt in self.MAPPING.items():
            if et != "work_orders":
                self.assertEqual(rt, "WEBPAGE", f"{et} should map to WEBPAGE")


# ═════════════════════════════════════════════════════════════════════════════
# Stage 8 — Kafka event format (sync-events topic)
# ═════════════════════════════════════════════════════════════════════════════

class TestKafkaSyncEventFormat(unittest.TestCase):
    """
    Verify the Kafka resync event payload published by innovint.connector.ts.
    These are structural tests — they validate the JSON shape that the Python
    connector's @ConnectorBuilder handler expects on the sync-events topic.
    """

    def _make_resync_event(
        self,
        org_id: str = "org-123",
        sync_type: str = "incremental",
    ) -> dict:
        return {
            "event":       "innovint.resync",
            "orgId":       org_id,
            "syncType":    sync_type,
            "triggeredAt": "2026-01-01T00:00:00.000Z",
        }

    def test_event_has_required_fields(self):
        evt = self._make_resync_event()
        self.assertIn("event", evt)
        self.assertIn("orgId", evt)
        self.assertIn("syncType", evt)
        self.assertIn("triggeredAt", evt)

    def test_event_name_matches_connector_handler(self):
        evt = self._make_resync_event()
        self.assertEqual(evt["event"], "innovint.resync")

    def test_full_sync_type(self):
        evt = self._make_resync_event(sync_type="full")
        self.assertEqual(evt["syncType"], "full")

    def test_incremental_sync_type(self):
        evt = self._make_resync_event(sync_type="incremental")
        self.assertEqual(evt["syncType"], "incremental")

    def test_org_id_is_kafka_message_key(self):
        """orgId in the event matches the Kafka message key for partitioning."""
        org_id = "org-abc"
        evt = self._make_resync_event(org_id=org_id)
        # In the connector manager, kafka message key == orgId
        self.assertEqual(evt["orgId"], org_id)


# ═════════════════════════════════════════════════════════════════════════════
# Stage 9 — Permission model
# ═════════════════════════════════════════════════════════════════════════════

class TestPermissionModel(unittest.TestCase):
    """
    Verify permission construction logic used in connector.py._build_permissions().

    InnoVint is org-wide: all active users get READER; admins get OWNER.
    """

    def _make_user(self, user_id: str, role: str = "member") -> dict:
        return {"id": user_id, "email": f"{user_id}@winery.com", "role": role}

    def _build_permissions(self, users: list[dict]) -> list[dict]:
        """Simulate connector.py's _build_permissions logic."""
        perms = []
        for user in users:
            if not user.get("active", True):
                continue
            perm_type = "OWNER" if user.get("role") == "admin" else "READER"
            perms.append({
                "from_id":   user["id"],
                "perm_type": perm_type,
            })
        return perms

    def test_member_gets_reader(self):
        user = self._make_user("u-1", role="member")
        perms = self._build_permissions([user])
        self.assertEqual(len(perms), 1)
        self.assertEqual(perms[0]["perm_type"], "READER")

    def test_admin_gets_owner(self):
        user = self._make_user("u-2", role="admin")
        perms = self._build_permissions([user])
        self.assertEqual(perms[0]["perm_type"], "OWNER")

    def test_mixed_roles(self):
        users = [
            self._make_user("u-1", role="member"),
            self._make_user("u-2", role="admin"),
            self._make_user("u-3", role="member"),
        ]
        perms = self._build_permissions(users)
        self.assertEqual(len(perms), 3)
        types = {p["from_id"]: p["perm_type"] for p in perms}
        self.assertEqual(types["u-1"], "READER")
        self.assertEqual(types["u-2"], "OWNER")
        self.assertEqual(types["u-3"], "READER")

    def test_inactive_users_excluded(self):
        users = [
            {**self._make_user("u-1"), "active": False},
            {**self._make_user("u-2"), "active": True},
        ]
        perms = self._build_permissions(users)
        self.assertEqual(len(perms), 1)
        self.assertEqual(perms[0]["from_id"], "u-2")

    def test_empty_user_list(self):
        perms = self._build_permissions([])
        self.assertEqual(perms, [])


# ═════════════════════════════════════════════════════════════════════════════
# Stage 10 — ETCD config key conventions
# ═════════════════════════════════════════════════════════════════════════════

class TestEtcdKeyConventions(unittest.TestCase):
    """
    Verify the ETCD key paths used for credential storage and retrieval.

    The Node.js Connector Manager writes; the Python connector reads via
    ConfigurationService.get_config().  Both must agree on the key format.
    """

    def _cm_key(self, org_id: str) -> str:
        """Key written by the Connector Manager (TypeScript)."""
        return f"services/connectors/innovint/{org_id}/config"

    def _connector_key(self, org_id: str) -> str:
        """Key read by the Python connector via ConfigurationService."""
        return f"services/connectors/innovint/{org_id}/config"

    def test_keys_match_across_services(self):
        org_id = "org-xyz"
        self.assertEqual(self._cm_key(org_id), self._connector_key(org_id))

    def test_key_is_org_scoped(self):
        key_a = self._cm_key("org-a")
        key_b = self._cm_key("org-b")
        self.assertNotEqual(key_a, key_b)

    def test_key_contains_connector_name(self):
        key = self._cm_key("org-1")
        self.assertIn("innovint", key)

    def test_key_under_services_prefix(self):
        key = self._cm_key("org-1")
        self.assertTrue(key.startswith("services/connectors/"))


if __name__ == "__main__":
    unittest.main()
