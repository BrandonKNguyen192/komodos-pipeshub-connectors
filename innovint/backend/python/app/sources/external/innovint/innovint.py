"""
InnoVint External API Client

Handles all HTTP communication with the InnoVint REST API (v1).
This module lives in sources/external/ per PipesHub convention — it is a
pure I/O layer with no PipesHub framework dependencies.

InnoVint API base: https://{tenant}.innovint.us/api/v1/

Supports:
- API-key authentication (X-API-Key header)
- Bearer-token authentication (Authorization: Bearer …)
- Automatic pagination (offset-based)
- Exponential-backoff retry on 5xx / network errors
- Respect for 429 Retry-After headers
- Bulk (paginated) list methods that return async generators
- Single-record fetch methods used by connector.py:stream_record()
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, AsyncIterator, Optional
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientTimeout

logger = logging.getLogger(__name__)

# ── Default tunables ──────────────────────────────────────────────────────────
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1.0   # base seconds for exponential back-off
RATE_LIMIT_DELAY = 0.1      # courtesy delay between consecutive requests


# ── Exceptions ────────────────────────────────────────────────────────────────

class InnovintAPIError(Exception):
    """Base exception for InnoVint API errors."""

    def __init__(
        self,
        message: str,
        status_code: int = 0,
        response_body: str = "",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class InnovintAuthError(InnovintAPIError):
    """401 / 403 — bad credentials or insufficient permissions."""


class InnovintNotFoundError(InnovintAPIError):
    """404 — the requested resource does not exist."""


class InnovintRateLimitError(InnovintAPIError):
    """429 — API rate limit exceeded."""


# ── Client ────────────────────────────────────────────────────────────────────

class InnovintClient:
    """
    Async HTTP client for the InnoVint REST API.

    Instantiate once per connector lifecycle (org/tenant pair) and use as an
    async context manager so the underlying aiohttp session is properly closed:

        async with InnovintClient(tenant="acme-winery", api_key="sk-…") as client:
            async for lot in client.list_lots():
                process(lot)

    The connector.py passes credentials obtained from ETCD via
    ConfigurationService, so this class has no framework dependency.
    """

    def __init__(
        self,
        tenant: str,
        api_key: Optional[str] = None,
        bearer_token: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        if not api_key and not bearer_token:
            raise ValueError(
                "InnovintClient requires either api_key or bearer_token"
            )
        self.tenant = tenant
        self.api_key = api_key
        self.bearer_token = bearer_token
        self.base_url = (
            base_url or f"https://{tenant}.innovint.us/api/v1"
        )
        self._timeout = ClientTimeout(total=timeout)
        self.page_size = page_size
        self.max_retries = max_retries
        self._session: Optional[aiohttp.ClientSession] = None

    # ── Context manager ───────────────────────────────────────────────────

    async def __aenter__(self) -> InnovintClient:
        await self._open_session()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def _open_session(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers=self._auth_headers(),
            )

    def _auth_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        elif self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        return headers

    async def close(self) -> None:
        """Close the underlying aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ── Low-level HTTP ────────────────────────────────────────────────────

    async def _get(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Perform a GET request with retry logic.

        Raises:
            InnovintAuthError     — 401 / 403
            InnovintNotFoundError — 404
            InnovintRateLimitError— 429 (after retries exhausted)
            InnovintAPIError      — any other non-2xx or network error
        """
        await self._open_session()
        assert self._session is not None  # always true after _open_session

        url = urljoin(self.base_url.rstrip("/") + "/", endpoint.lstrip("/"))

        for attempt in range(self.max_retries):
            try:
                delay = (
                    DEFAULT_RETRY_DELAY * (2 ** attempt)
                    if attempt > 0
                    else RATE_LIMIT_DELAY
                )
                await asyncio.sleep(delay)

                async with self._session.get(url, params=params) as resp:
                    if resp.status == 200:
                        return await resp.json()

                    body = await resp.text()

                    if resp.status == 401:
                        raise InnovintAuthError(
                            "Authentication failed — check API key",
                            status_code=resp.status,
                            response_body=body,
                        )
                    if resp.status == 403:
                        raise InnovintAuthError(
                            f"Access forbidden for {endpoint}",
                            status_code=resp.status,
                            response_body=body,
                        )
                    if resp.status == 404:
                        raise InnovintNotFoundError(
                            f"Resource not found: {endpoint}",
                            status_code=404,
                            response_body=body,
                        )
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", "5"))
                        logger.warning(
                            "Rate-limited by InnoVint; sleeping %.0fs "
                            "(attempt %d/%d)",
                            retry_after,
                            attempt + 1,
                            self.max_retries,
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    if resp.status >= 500:
                        logger.warning(
                            "InnoVint server error %d on %s (attempt %d/%d)",
                            resp.status,
                            url,
                            attempt + 1,
                            self.max_retries,
                        )
                        if attempt < self.max_retries - 1:
                            continue
                        raise InnovintAPIError(
                            f"Server error {resp.status} after "
                            f"{self.max_retries} retries",
                            status_code=resp.status,
                            response_body=body,
                        )

                    raise InnovintAPIError(
                        f"Unexpected status {resp.status} from {endpoint}",
                        status_code=resp.status,
                        response_body=body,
                    )

            except InnovintAPIError:
                # Don't swallow our own typed exceptions — let them propagate.
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning(
                    "Network error fetching %s: %s (attempt %d/%d)",
                    url,
                    exc,
                    attempt + 1,
                    self.max_retries,
                )
                if attempt == self.max_retries - 1:
                    raise InnovintAPIError(
                        f"Network error after {self.max_retries} retries: {exc}"
                    ) from exc

        raise InnovintAPIError("Exhausted all retries without a response")

    async def _paginate(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        results_key: str = "results",
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Yield individual items from a paginated list endpoint.

        InnoVint uses offset + limit pagination.  The response may carry a
        ``total`` or ``count`` field; if absent we stop when a short page is
        returned.
        """
        params = dict(params or {})
        params["limit"] = self.page_size
        offset = 0

        while True:
            params["offset"] = offset
            response = await self._get(endpoint, params=params)

            # InnoVint may wrap results under 'results', 'data', or at root
            items: list[dict[str, Any]] = (
                response.get(results_key)
                or response.get("data")
                or []
            )
            if not items:
                break

            for item in items:
                yield item

            total = response.get("total") or response.get("count")
            fetched = offset + len(items)
            if total is not None and fetched >= total:
                break
            if len(items) < self.page_size:
                break

            offset = fetched

    # ── Lots ──────────────────────────────────────────────────────────────

    def list_lots(
        self,
        status: Optional[str] = None,
        vintage: Optional[int] = None,
        updated_since: Optional[datetime] = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Yield all lots, optionally filtered.

        Args:
            status:        e.g. "active", "archived"
            vintage:       4-digit vintage year
            updated_since: only return lots modified after this timestamp
        """
        params: dict[str, Any] = {}
        if status:
            params["status"] = status
        if vintage:
            params["vintage"] = vintage
        if updated_since:
            params["updated_since"] = updated_since.isoformat()
        return self._paginate("lots", params=params)

    async def get_lot(self, lot_id: str) -> dict[str, Any]:
        """Fetch a single lot by its InnoVint ID."""
        return await self._get(f"lots/{lot_id}")

    async def get_lot_history(self, lot_id: str) -> list[dict[str, Any]]:
        """Return the action/event history list for a lot."""
        resp = await self._get(f"lots/{lot_id}/history")
        return resp.get("results") or resp.get("data") or []

    # ── Vessels ───────────────────────────────────────────────────────────

    def list_vessels(
        self,
        vessel_type: Optional[str] = None,
        location: Optional[str] = None,
        updated_since: Optional[datetime] = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield all vessels, optionally filtered by type, location, or recency."""
        params: dict[str, Any] = {}
        if vessel_type:
            params["type"] = vessel_type
        if location:
            params["location"] = location
        if updated_since:
            params["updated_since"] = updated_since.isoformat()
        return self._paginate("vessels", params=params)

    async def get_vessel(self, vessel_id: str) -> dict[str, Any]:
        """Fetch a single vessel by its InnoVint ID."""
        return await self._get(f"vessels/{vessel_id}")

    # ── Analyses ──────────────────────────────────────────────────────────

    def list_analyses(
        self,
        lot_id: Optional[str] = None,
        vessel_id: Optional[str] = None,
        updated_since: Optional[datetime] = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield lab analyses, optionally filtered by lot, vessel, or recency."""
        params: dict[str, Any] = {}
        if lot_id:
            params["lot_id"] = lot_id
        if vessel_id:
            params["vessel_id"] = vessel_id
        if updated_since:
            params["updated_since"] = updated_since.isoformat()
        return self._paginate("analyses", params=params)

    async def get_analysis(self, analysis_id: str) -> dict[str, Any]:
        """Fetch a single analysis result by its InnoVint ID."""
        return await self._get(f"analyses/{analysis_id}")

    # ── Work Orders ───────────────────────────────────────────────────────

    def list_work_orders(
        self,
        status: Optional[str] = None,
        lot_id: Optional[str] = None,
        updated_since: Optional[datetime] = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield work orders, optionally filtered by status, lot, or recency."""
        params: dict[str, Any] = {}
        if status:
            params["status"] = status
        if lot_id:
            params["lot_id"] = lot_id
        if updated_since:
            params["updated_since"] = updated_since.isoformat()
        return self._paginate("work-orders", params=params)

    async def get_work_order(self, work_order_id: str) -> dict[str, Any]:
        """Fetch a single work order by its InnoVint ID."""
        return await self._get(f"work-orders/{work_order_id}")

    # ── Case Goods ────────────────────────────────────────────────────────

    def list_case_goods(
        self,
        updated_since: Optional[datetime] = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield case goods (finished / bottled products)."""
        params: dict[str, Any] = {}
        if updated_since:
            params["updated_since"] = updated_since.isoformat()
        return self._paginate("case-goods", params=params)

    async def get_case_good(self, case_good_id: str) -> dict[str, Any]:
        """Fetch a single case good by its InnoVint ID."""
        return await self._get(f"case-goods/{case_good_id}")

    # ── Costs ─────────────────────────────────────────────────────────────

    def list_costs(
        self,
        lot_id: Optional[str] = None,
        updated_since: Optional[datetime] = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield cost entries, optionally filtered by lot or recency."""
        params: dict[str, Any] = {}
        if lot_id:
            params["lot_id"] = lot_id
        if updated_since:
            params["updated_since"] = updated_since.isoformat()
        return self._paginate("costs", params=params)

    async def get_cost(self, cost_id: str) -> dict[str, Any]:
        """Fetch a single cost entry by its InnoVint ID."""
        return await self._get(f"costs/{cost_id}")

    # ── Users / Org ───────────────────────────────────────────────────────

    def list_users(self) -> AsyncIterator[dict[str, Any]]:
        """
        Yield all users in the organisation.

        Used by connector.py to build Permission edges — every active user
        gets READER access to all InnoVint records.
        """
        return self._paginate("users", results_key="users")

    async def get_user(self, user_id: str) -> dict[str, Any]:
        """Fetch a single user profile."""
        return await self._get(f"users/{user_id}")

    # ── Health ────────────────────────────────────────────────────────────

    async def health_check(self) -> bool:
        """
        Verify API connectivity and credential validity.

        Returns True if the API responds successfully, False otherwise.
        Does NOT raise — safe to call inside a health-check endpoint.
        """
        try:
            await self._get("lots", params={"limit": 1})
            return True
        except InnovintAPIError:
            return False
