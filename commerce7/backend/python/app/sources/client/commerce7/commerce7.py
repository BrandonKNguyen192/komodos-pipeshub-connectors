# Copyright 2026 Komodos, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

"""
Commerce7 REST API Client

Commerce7 uses Basic Auth with App ID (username) and App Secret Key (password).
The tenant ID is passed as a header with every request.

API Base URL: https://api.commerce7.com/v1/
Rate Limit: 100 requests per minute per tenant (cursor-based pagination has no rate limits).
"""

import base64
import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel  # type: ignore

from app.sources.client.http.http_client import HTTPClient
from app.sources.client.http.http_request import HTTPRequest
from app.sources.client.iclient import IClient


COMMERCE7_API_BASE = "https://api.commerce7.com/v1"


class Commerce7Response(BaseModel):
    """Standardized Commerce7 API response wrapper."""
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    total: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


class Commerce7RESTClient(HTTPClient):
    """Commerce7 REST client using Basic Auth.

    Commerce7 authentication format:
        Authorization: Basic base64(appId:appSecretKey)
        tenant: <tenant_id>  (header)

    Args:
        app_id: The Commerce7 App ID (username for Basic Auth)
        app_secret_key: The Commerce7 App Secret Key (password for Basic Auth)
        tenant_id: The Commerce7 tenant identifier
    """

    def __init__(self, app_id: str, app_secret_key: str, tenant_id: str) -> None:
        # Build Basic Auth token
        credentials = f"{app_id}:{app_secret_key}"
        token = base64.b64encode(credentials.encode()).decode()
        super().__init__(token, "Basic")

        self.app_id = app_id
        self.tenant_id = tenant_id
        self.base_url = COMMERCE7_API_BASE

        # Add Commerce7-specific headers
        self.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "tenant": tenant_id,
        })

    def get_base_url(self) -> str:
        return self.base_url


class Commerce7Config(BaseModel):
    """Configuration for Commerce7 REST client.

    Args:
        app_id: The Commerce7 App ID
        app_secret_key: The Commerce7 App Secret Key
        tenant_id: The Commerce7 tenant identifier
    """
    app_id: str
    app_secret_key: str
    tenant_id: str

    def create_client(self) -> Commerce7RESTClient:
        return Commerce7RESTClient(self.app_id, self.app_secret_key, self.tenant_id)

    def to_dict(self) -> dict:
        return self.model_dump()


class Commerce7Client(IClient):
    """High-level Commerce7 API client with methods for each resource type."""

    def __init__(self, client: Commerce7RESTClient) -> None:
        self.client = client
        self.logger = logging.getLogger(__name__)

    def get_client(self) -> Commerce7RESTClient:
        return self.client

    def get_base_url(self) -> str:
        return self.client.get_base_url()

    @classmethod
    def build_with_config(cls, config: Commerce7Config) -> "Commerce7Client":
        client = config.create_client()
        return cls(client)

    @classmethod
    async def build_and_validate(cls, config: Commerce7Config) -> "Commerce7Client":
        """Build client and validate connection by making a test API call."""
        instance = cls.build_with_config(config)
        # Test with a minimal customers request
        response = await instance.list_customers(limit=1)
        if not response.success:
            raise ValueError(f"Commerce7 connection validation failed: {response.error}")
        return instance

    # ─── Generic helpers ──────────────────────────────────────────────

    async def _get(self, endpoint: str, params: Optional[Dict] = None) -> Commerce7Response:
        """Execute a GET request against the Commerce7 API."""
        url = f"{self.client.get_base_url()}/{endpoint.lstrip('/')}"
        request = HTTPRequest(url=url, method="GET", query=params or {})
        try:
            response = await self.client.execute(request)
            if response.status == 200:
                data = response.json()
                # Commerce7 list endpoints return objects with a plural key + "total"
                total = data.get("total", 0) if isinstance(data, dict) else 0
                return Commerce7Response(success=True, data=data, total=total)
            else:
                return Commerce7Response(
                    success=False,
                    error=f"HTTP {response.status}: {response.text()[:500]}"
                )
        except Exception as e:
            return Commerce7Response(success=False, error=str(e))

    async def _get_paginated(
        self,
        endpoint: str,
        data_key: str,
        params: Optional[Dict] = None,
        limit: int = 50,
        cursor: Optional[str] = None,
    ) -> Commerce7Response:
        """Fetch a single page using cursor-based pagination.

        Commerce7 supports cursor-based pagination which has no rate limits:
        - Use `cursor` parameter to paginate
        - Response includes `cursor` for next page
        """
        p = dict(params or {})
        if cursor:
            p["cursor"] = cursor
        else:
            p["limit"] = str(limit)
        return await self._get(endpoint, p)

    async def _get_all_pages(
        self,
        endpoint: str,
        data_key: str,
        params: Optional[Dict] = None,
        page_size: int = 500,
    ) -> Commerce7Response:
        """Fetch ALL records across all pages using cursor-based pagination.

        NOTE: Prefer iter_all_pages() for large datasets to avoid loading
        all records into memory at once.
        """
        all_items: List[Any] = []
        cursor: Optional[str] = None
        total = 0

        while True:
            response = await self._get_paginated(
                endpoint, data_key, params=params, limit=page_size, cursor=cursor
            )
            if not response.success:
                return response

            data = response.data or {}
            items = data.get(data_key, [])
            all_items.extend(items)
            total = data.get("total", total)

            # Get next cursor
            cursor = data.get("cursor")
            if not cursor or not items:
                break

        return Commerce7Response(success=True, data={data_key: all_items, "total": total}, total=total)

    async def iter_all_pages(
        self,
        endpoint: str,
        data_key: str,
        params: Optional[Dict] = None,
        page_size: int = 500,
    ):
        """Async generator that yields one page of records at a time.

        Use this instead of _get_all_pages() for large datasets — each page
        is yielded and can be processed/upserted immediately, so only
        page_size records are held in memory at any point.

        Args:
            endpoint: Commerce7 API endpoint (e.g. "customer")
            data_key: Key in the response dict holding the list (e.g. "customers")
            params: Optional extra query parameters
            page_size: Records per page (max 500; cursor pagination has no rate limit)

        Yields:
            List[Dict]: One page of raw API records

        Raises:
            RuntimeError: On API error so the caller can handle/log it
        """
        cursor: Optional[str] = None

        while True:
            response = await self._get_paginated(
                endpoint, data_key, params=params, limit=page_size, cursor=cursor
            )
            if not response.success:
                raise RuntimeError(
                    f"Commerce7 API error fetching {endpoint}: {response.error}"
                )

            data = response.data or {}
            items: List[Any] = data.get(data_key, [])

            if not items:
                break

            yield items

            cursor = data.get("cursor")
            if not cursor:
                break

    # ─── Customers ────────────────────────────────────────────────────

    async def list_customers(
        self, limit: int = 50, cursor: Optional[str] = None, **params
    ) -> Commerce7Response:
        p = dict(params)
        return await self._get_paginated("customer", "customers", p, limit, cursor)

    async def get_all_customers(self, page_size: int = 500) -> Commerce7Response:
        return await self._get_all_pages("customer", "customers", page_size=page_size)

    async def get_customer(self, customer_id: str) -> Commerce7Response:
        return await self._get(f"customer/{customer_id}")

    # ─── Orders ───────────────────────────────────────────────────────

    async def list_orders(
        self, limit: int = 50, cursor: Optional[str] = None, **params
    ) -> Commerce7Response:
        p = dict(params)
        return await self._get_paginated("order", "orders", p, limit, cursor)

    async def get_all_orders(self, page_size: int = 500) -> Commerce7Response:
        return await self._get_all_pages("order", "orders", page_size=page_size)

    async def get_order(self, order_id: str) -> Commerce7Response:
        return await self._get(f"order/{order_id}")

    # ─── Products ─────────────────────────────────────────────────────

    async def list_products(
        self, limit: int = 50, cursor: Optional[str] = None, **params
    ) -> Commerce7Response:
        p = dict(params)
        return await self._get_paginated("product", "products", p, limit, cursor)

    async def get_all_products(self, page_size: int = 500) -> Commerce7Response:
        return await self._get_all_pages("product", "products", page_size=page_size)

    async def get_product(self, product_id: str) -> Commerce7Response:
        return await self._get(f"product/{product_id}")

    # ─── Clubs ────────────────────────────────────────────────────────

    async def list_clubs(
        self, limit: int = 50, cursor: Optional[str] = None, **params
    ) -> Commerce7Response:
        p = dict(params)
        return await self._get_paginated("club", "clubs", p, limit, cursor)

    async def get_all_clubs(self, page_size: int = 500) -> Commerce7Response:
        return await self._get_all_pages("club", "clubs", page_size=page_size)

    async def get_club(self, club_id: str) -> Commerce7Response:
        return await self._get(f"club/{club_id}")

    # ─── Club Memberships ─────────────────────────────────────────────

    async def list_club_memberships(
        self, limit: int = 50, cursor: Optional[str] = None, **params
    ) -> Commerce7Response:
        p = dict(params)
        return await self._get_paginated("club-membership", "clubMemberships", p, limit, cursor)

    async def get_all_club_memberships(self, page_size: int = 500) -> Commerce7Response:
        return await self._get_all_pages("club-membership", "clubMemberships", page_size=page_size)

    # ─── Reservations ─────────────────────────────────────────────────

    async def list_reservations(
        self, limit: int = 50, cursor: Optional[str] = None, **params
    ) -> Commerce7Response:
        p = dict(params)
        return await self._get_paginated("reservation", "reservations", p, limit, cursor)

    async def get_all_reservations(self, page_size: int = 500) -> Commerce7Response:
        return await self._get_all_pages("reservation", "reservations", page_size=page_size)

    # ─── Inventory ────────────────────────────────────────────────────

    async def list_inventory(
        self, limit: int = 50, cursor: Optional[str] = None, **params
    ) -> Commerce7Response:
        p = dict(params)
        return await self._get_paginated("inventory", "inventoryLocations", p, limit, cursor)

    async def get_all_inventory(self, page_size: int = 500) -> Commerce7Response:
        return await self._get_all_pages("inventory", "inventoryLocations", page_size=page_size)

    # ─── Webhooks ─────────────────────────────────────────────────────

    async def list_webhooks(self) -> Commerce7Response:
        return await self._get("web-hook")

    async def create_webhook(self, url: str, object_type: str, action: str) -> Commerce7Response:
        """Create a webhook for real-time sync.

        Args:
            url: The URL to send webhook events to
            object_type: The object type (Customer, Product, Order, Group)
            action: The action (Create, Update, Delete, BulkUpdate)
        """
        endpoint = f"{self.client.get_base_url()}/web-hook"
        request = HTTPRequest(
            url=endpoint,
            method="POST",
            body={
                "url": url,
                "objectType": object_type,
                "action": action,
            }
        )
        try:
            response = await self.client.execute(request)
            if response.status in (200, 201):
                return Commerce7Response(success=True, data=response.json())
            else:
                return Commerce7Response(
                    success=False,
                    error=f"HTTP {response.status}: {response.text()[:500]}"
                )
        except Exception as e:
            return Commerce7Response(success=False, error=str(e))

    # ─── Cleanup ──────────────────────────────────────────────────────

    async def close(self) -> None:
        await self.client.close()
