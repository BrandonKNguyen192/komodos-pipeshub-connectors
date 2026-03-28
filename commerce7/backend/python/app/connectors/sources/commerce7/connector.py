# Copyright 2026 Komodos, Inc.
# Licensed under the Apache License, Version 2.0
# http://www.apache.org/licenses/LICENSE-2.0

"""
Commerce7 Connector for PipesHub AI

Syncs customers, orders, products, clubs, club memberships, reservations,
and inventory from a Commerce7 tenant into PipesHub AI as WebpageRecords.

Commerce7 API:
  - Base URL: https://api.commerce7.com/v1/
  - Auth: Basic Auth (App ID : App Secret Key) + tenant header
  - Rate Limit: 100 req/min per tenant (cursor pagination is unlimited)
  - Webhooks: Supported for Customer, Product, Order, Group (Create/Update/Delete)

Documentation:
  - https://developer.commerce7.com/docs/commerce7-developer-docs
  - https://api-docs.commerce7.com/docs/getting-started
"""

import json
import uuid
from datetime import datetime, timezone
from logging import Logger
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from app.config.configuration_service import ConfigurationService
from app.config.constants.arangodb import (
    Connectors,
    MimeTypes,
    OriginTypes,
)
from app.config.constants.http_status_code import HttpStatusCode
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
from app.connectors.core.registry.auth_builder import (
    AuthBuilder,
    AuthType,
)
from app.connectors.core.registry.connector_builder import (
    AuthField,
    CommonFields,
    ConnectorBuilder,
    ConnectorScope,
    DocumentationLink,
    SyncStrategy,
)
from app.connectors.core.registry.filters import (
    FilterCategory,
    FilterCollection,
    FilterField,
    FilterOption,
    FilterType,
    OptionSourceType,
    load_connector_filters,
)
from app.connectors.sources.commerce7.common.apps import Commerce7App
from app.models.entities import (
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
    WebpageRecord,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.sources.client.commerce7.commerce7 import (
    Commerce7Client,
    Commerce7Config,
    Commerce7Response,
)
from app.utils.streaming import create_stream_record_response


# ─── Resource type constants ─────────────────────────────────────────
RESOURCE_CUSTOMERS = "customers"
RESOURCE_ORDERS = "orders"
RESOURCE_PRODUCTS = "products"
RESOURCE_CLUBS = "clubs"
RESOURCE_CLUB_MEMBERSHIPS = "clubMemberships"
RESOURCE_RESERVATIONS = "reservations"
RESOURCE_INVENTORY = "inventory"

ALL_RESOURCES = [
    RESOURCE_CUSTOMERS,
    RESOURCE_ORDERS,
    RESOURCE_PRODUCTS,
    RESOURCE_CLUBS,
    RESOURCE_CLUB_MEMBERSHIPS,
    RESOURCE_RESERVATIONS,
    RESOURCE_INVENTORY,
]

# ─── Class-level constants (built once, never per-call) ───────────────────────

# Maps resource type → Commerce7 API endpoint
_RESOURCE_ENDPOINTS = {
    RESOURCE_CUSTOMERS: "customer",
    RESOURCE_ORDERS: "order",
    RESOURCE_PRODUCTS: "product",
    RESOURCE_CLUBS: "club",
    RESOURCE_CLUB_MEMBERSHIPS: "club-membership",
    RESOURCE_RESERVATIONS: "reservation",
    RESOURCE_INVENTORY: "inventory",
}

# Maps resource type → key in the API response dict
_RESOURCE_DATA_KEYS = {
    RESOURCE_CUSTOMERS: "customers",
    RESOURCE_ORDERS: "orders",
    RESOURCE_PRODUCTS: "products",
    RESOURCE_CLUBS: "clubs",
    RESOURCE_CLUB_MEMBERSHIPS: "clubMemberships",
    RESOURCE_RESERVATIONS: "reservations",
    RESOURCE_INVENTORY: "inventoryLocations",
}

# Maps resource type → human-readable RecordGroup name
_RESOURCE_DISPLAY_NAMES = {
    RESOURCE_CUSTOMERS: "Commerce7 Customers",
    RESOURCE_ORDERS: "Commerce7 Orders",
    RESOURCE_PRODUCTS: "Commerce7 Products",
    RESOURCE_CLUBS: "Commerce7 Clubs",
    RESOURCE_CLUB_MEMBERSHIPS: "Commerce7 Club Memberships",
    RESOURCE_RESERVATIONS: "Commerce7 Reservations",
    RESOURCE_INVENTORY: "Commerce7 Inventory",
}

# Maps resource type → Commerce7 admin URL path segment
_RESOURCE_URL_PATHS = {
    RESOURCE_CUSTOMERS: "customer",
    RESOURCE_ORDERS: "order",
    RESOURCE_PRODUCTS: "product",
    RESOURCE_CLUBS: "club",
    RESOURCE_CLUB_MEMBERSHIPS: "club-membership",
    RESOURCE_RESERVATIONS: "reservation",
    RESOURCE_INVENTORY: "inventory",
}


@ConnectorBuilder("Commerce7")\
    .in_group("Commerce7")\
    .with_supported_auth_types("API_TOKEN")\
    .with_description("Sync customers, orders, products, clubs, reservations, and inventory from Commerce7")\
    .with_categories(["E-Commerce", "Wine", "CRM"])\
    .with_scopes([ConnectorScope.TEAM.value])\
    .with_auth([
        AuthBuilder.type(AuthType.API_TOKEN).fields([
            AuthField(
                name="app_id",
                display_name="App ID",
                placeholder="Your Commerce7 App ID",
                description="The App ID from your Commerce7 developer account or app registration",
                field_type="TEXT",
                max_length=200,
            ),
            AuthField(
                name="app_secret_key",
                display_name="App Secret Key",
                placeholder="Your Commerce7 App Secret Key",
                description="The App Secret Key from your Commerce7 developer account",
                field_type="PASSWORD",
                is_secret=True,
                max_length=200,
            ),
            AuthField(
                name="tenant_id",
                display_name="Tenant ID",
                placeholder="your-winery-tenant",
                description="Your Commerce7 tenant identifier (found in your Commerce7 admin URL)",
                field_type="TEXT",
                max_length=200,
            ),
        ])
    ])\
    .configure(lambda builder: builder
        .with_icon("/assets/icons/connectors/commerce7.svg")
        .add_documentation_link(DocumentationLink(
            "Commerce7 Developer Docs",
            "https://developer.commerce7.com/docs/commerce7-developer-docs",
            "docs"
        ))
        .add_documentation_link(DocumentationLink(
            "Commerce7 API Reference",
            "https://api-docs.commerce7.com/docs/getting-started",
            "api"
        ))
        .add_filter_field(CommonFields.modified_date_filter("Filter records by modification date."))
        .add_filter_field(CommonFields.created_date_filter("Filter records by creation date."))
        .add_filter_field(CommonFields.enable_manual_sync_filter())
        .add_filter_field(FilterField(
            name="resource_types",
            display_name="Resource Types",
            description="Select which Commerce7 resource types to sync",
            filter_type=FilterType.LIST,
            category=FilterCategory.SYNC,
            default_value=[],
            option_source_type=OptionSourceType.STATIC,
            options=[
                FilterOption(value=RESOURCE_CUSTOMERS, label="Customers"),
                FilterOption(value=RESOURCE_ORDERS, label="Orders"),
                FilterOption(value=RESOURCE_PRODUCTS, label="Products"),
                FilterOption(value=RESOURCE_CLUBS, label="Clubs"),
                FilterOption(value=RESOURCE_CLUB_MEMBERSHIPS, label="Club Memberships"),
                FilterOption(value=RESOURCE_RESERVATIONS, label="Reservations"),
                FilterOption(value=RESOURCE_INVENTORY, label="Inventory"),
            ],
        ))
        .with_sync_strategies([SyncStrategy.SCHEDULED, SyncStrategy.MANUAL])
        .with_scheduled_config(True, 15)
        .with_sync_support(True)
        .with_agent_support(False)
    )\
    .build_decorator()
class Commerce7Connector(BaseConnector):
    """
    Connector for synchronizing data from a Commerce7 tenant.

    Syncs the following resources as WebpageRecords:
      - Customers
      - Orders
      - Products
      - Clubs
      - Club Memberships
      - Reservations
      - Inventory

    Each resource type is stored as a RecordGroup, and individual records
    within each type are stored as WebpageRecords with structured JSON content.
    """

    def __init__(
        self,
        logger: Logger,
        data_entities_processor: DataSourceEntitiesProcessor,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
    ) -> None:
        super().__init__(
            Commerce7App(connector_id),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
        )

        self.connector_name = Connectors.COMMERCE7
        self.connector_id = connector_id

        # API client
        self.commerce7_client: Optional[Commerce7Client] = None
        self.tenant_id: Optional[str] = None

        # Sync points
        self._create_sync_points()

        # Filters
        self.sync_filters: FilterCollection = FilterCollection()
        self.indexing_filters: FilterCollection = FilterCollection()

        # Computed once — reused for every record during sync
        self._mime_type: str = (
            MimeTypes.JSON if hasattr(MimeTypes, "JSON") else "application/json"
        )
        # Org-wide READER permission is identical for every Commerce7 record;
        # build the list once rather than allocating a new object per record.
        self._org_permission: List[Permission] = []  # populated after org_id is known

    def _create_sync_points(self) -> None:
        """Initialize sync points for tracking changes."""
        def _create_sync_point(sync_data_point_type: SyncDataPointType) -> SyncPoint:
            return SyncPoint(
                connector_id=self.connector_id,
                org_id=self.data_entities_processor.org_id,
                sync_data_point_type=sync_data_point_type,
                data_store_provider=self.data_store_provider,
            )

        self.record_sync_point = _create_sync_point(SyncDataPointType.RECORDS)

    # ─── Initialization ───────────────────────────────────────────────

    async def init(self) -> bool:
        """Initialize the connector with credentials from config."""
        try:
            config = await self.config_service.get_config(
                f"/services/connectors/{self.connector_id}/config"
            )
            if not config:
                self.logger.error("Commerce7 configuration not found.")
                return False

            credentials = config.get("auth", {})
            app_id = credentials.get("app_id")
            app_secret_key = credentials.get("app_secret_key")
            self.tenant_id = credentials.get("tenant_id")

            if not all([app_id, app_secret_key, self.tenant_id]):
                self.logger.error(
                    "Commerce7 app_id, app_secret_key, or tenant_id not found in configuration."
                )
                return False

            c7_config = Commerce7Config(
                app_id=app_id,
                app_secret_key=app_secret_key,
                tenant_id=self.tenant_id,
            )

            try:
                self.commerce7_client = await Commerce7Client.build_and_validate(c7_config)
            except ValueError as e:
                self.logger.error(f"Failed to initialize Commerce7 client: {e}", exc_info=True)
                return False

            # Build the shared permission object now that org_id is available
            self._org_permission = [
                Permission(
                    type=PermissionType.READER,
                    entity_type=EntityType.ORG,
                    org_id=self.data_entities_processor.org_id,
                )
            ]

            self.logger.info("Commerce7 client initialized successfully.")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize Commerce7 client: {e}", exc_info=True)
            return False

    @classmethod
    async def create_connector(
        cls,
        logger: Logger,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
    ) -> "Commerce7Connector":
        """Factory method used by ConnectorFactory to instantiate this connector."""
        data_entities_processor = DataSourceEntitiesProcessor(
            logger, data_store_provider, config_service
        )
        await data_entities_processor.initialize()
        return cls(logger, data_entities_processor, data_store_provider, config_service, connector_id)

    async def test_connection_and_access(self) -> bool:
        """Test connectivity by listing customers."""
        if not self.commerce7_client:
            self.logger.error("Commerce7 client not initialized.")
            return False
        try:
            response = await self.commerce7_client.list_customers(limit=1)
            if response.success:
                self.logger.info("Commerce7 connection test successful.")
                return True
            self.logger.error(f"Commerce7 connection test failed: {response.error}")
            return False
        except Exception as e:
            self.logger.error(f"Commerce7 connection test failed: {e}", exc_info=True)
            return False

    # ─── Sync ─────────────────────────────────────────────────────────

    async def run_sync(self) -> None:
        """Run a full synchronization from the Commerce7 tenant."""
        try:
            self.logger.info("Starting Commerce7 full sync.")

            self.sync_filters, self.indexing_filters = await load_connector_filters(
                self.config_service, "commerce7", self.connector_id, self.logger
            )

            # Determine which resource types to sync
            resource_types = self._get_enabled_resource_types()
            self.logger.info(f"Syncing resource types: {resource_types}")

            for resource_type in resource_types:
                self.logger.info(f"Syncing {resource_type}...")
                await self._sync_resource_type(resource_type)
                self.logger.info(f"Finished syncing {resource_type}.")

            self.logger.info("Commerce7 full sync completed.")

        except Exception as ex:
            self.logger.error(f"Error in Commerce7 connector run: {ex}", exc_info=True)
            raise

    async def run_incremental_sync(self) -> None:
        """Run incremental synchronization.

        Commerce7 does not provide delta links or change tracking natively.
        We re-fetch all data and use external_revision_id (updatedAt timestamp)
        to detect changes. The DataSourceEntitiesProcessor handles deduplication.
        """
        await self.run_sync()

    def _get_enabled_resource_types(self) -> List[str]:
        """Get the list of resource types to sync based on filters."""
        # Check if specific resource types are configured in sync filters
        filter_field = self.sync_filters.get_filter("resource_types")
        if filter_field and filter_field.value:
            return filter_field.value
        # Default: sync all resource types
        return ALL_RESOURCES

    # ─── Resource-type dispatch ───────────────────────────────────────

    async def _sync_resource_type(self, resource_type: str) -> None:
        """Sync all records for a given resource type.

        Uses iter_all_pages() to process one page at a time (up to 500 records),
        upsert it immediately, then discard it — keeping RAM usage flat regardless
        of how many total records exist in the tenant.
        """
        if not self.commerce7_client:
            raise RuntimeError("Commerce7 client not initialized.")

        endpoint = _RESOURCE_ENDPOINTS.get(resource_type)
        data_key = _RESOURCE_DATA_KEYS.get(resource_type)
        if not endpoint or not data_key:
            self.logger.warning(f"Unknown resource type: {resource_type}")
            return

        record_group = self._build_record_group(resource_type)
        total_upserted = 0

        try:
            async for page_items in self.commerce7_client.iter_all_pages(
                endpoint, data_key
            ):
                records_with_permissions: List[Tuple[WebpageRecord, List[Permission]]] = [
                    (self._transform_item_to_record(item, resource_type, record_group),
                     self._org_permission)
                    for item in page_items
                ]
                await self.data_entities_processor.on_new_records(records_with_permissions)
                total_upserted += len(records_with_permissions)
                self.logger.debug(
                    f"Upserted page of {len(records_with_permissions)} {resource_type} "
                    f"(running total: {total_upserted})"
                )

        except RuntimeError as e:
            self.logger.error(f"Failed to sync {resource_type}: {e}")
            return

        self.logger.info(f"Finished syncing {resource_type}: {total_upserted} records upserted.")

        # Update sync point
        sync_key = generate_record_sync_point_key("commerce7", resource_type, "global")
        await self.record_sync_point.update_sync_point(
            sync_key,
            {
                "last_sync_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                "record_count": total_upserted,
            },
        )

    # ─── Data transformation ──────────────────────────────────────────

    def _build_record_group(self, resource_type: str) -> RecordGroup:
        """Build a RecordGroup for a Commerce7 resource type."""
        return RecordGroup(
            org_id=self.data_entities_processor.org_id,
            record_group_name=_RESOURCE_DISPLAY_NAMES.get(
                resource_type, f"Commerce7 {resource_type}"
            ),
            record_group_type=RecordGroupType.CONTAINER,
            external_record_group_id=f"commerce7-{self.tenant_id}-{resource_type}",
            connector_name=Connectors.COMMERCE7,
            connector_id=self.connector_id,
        )

    def _transform_item_to_record(
        self, item: Dict[str, Any], resource_type: str, record_group: RecordGroup
    ) -> WebpageRecord:
        """Transform a Commerce7 API item into a WebpageRecord."""
        item_id = item.get("id", str(uuid.uuid4()))
        record_name = self._get_record_name(item, resource_type)

        # Use updatedAt as revision ID for change detection
        updated_at_str = item.get("updatedAt") or item.get("createdAt")
        created_at_str = item.get("createdAt")

        return WebpageRecord(
            id=str(uuid.uuid4()),
            org_id=self.data_entities_processor.org_id,
            record_name=record_name,
            record_type=RecordType.WEBPAGE,
            external_record_id=f"{resource_type}/{item_id}",
            external_revision_id=updated_at_str or "",
            external_record_group_id=record_group.external_record_group_id,
            connector_name=Connectors.COMMERCE7,
            connector_id=self.connector_id,
            origin=OriginTypes.CONNECTOR,
            version=0,
            mime_type=self._mime_type,
            source_created_at=self._parse_datetime(created_at_str),
            source_updated_at=self._parse_datetime(updated_at_str),
            weburl=self._build_web_url(item, resource_type),
            signed_url=None,
        )

    def _get_record_name(self, item: Dict[str, Any], resource_type: str) -> str:
        """Derive a human-readable record name from the item."""
        if resource_type == RESOURCE_CUSTOMERS:
            first = item.get("firstName", "")
            last = item.get("lastName", "")
            email = item.get("emails", [{}])[0].get("email", "") if item.get("emails") else ""
            name = f"{first} {last}".strip()
            return name if name else email or f"Customer {item.get('id', 'unknown')}"

        if resource_type == RESOURCE_ORDERS:
            order_number = item.get("orderNumber") or item.get("id", "unknown")
            return f"Order #{order_number}"

        if resource_type == RESOURCE_PRODUCTS:
            return item.get("title", f"Product {item.get('id', 'unknown')}")

        if resource_type == RESOURCE_CLUBS:
            return item.get("title", f"Club {item.get('id', 'unknown')}")

        if resource_type == RESOURCE_CLUB_MEMBERSHIPS:
            club_title = item.get("clubTitle", "")
            customer_name = ""
            customer = item.get("customer", {})
            if customer:
                customer_name = f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip()
            return f"{club_title} - {customer_name}".strip(" -") or f"Membership {item.get('id', 'unknown')}"

        if resource_type == RESOURCE_RESERVATIONS:
            reservation_type = item.get("type", "Reservation")
            date = item.get("date", "")
            return f"{reservation_type} on {date}" if date else f"Reservation {item.get('id', 'unknown')}"

        if resource_type == RESOURCE_INVENTORY:
            return item.get("title", f"Inventory Location {item.get('id', 'unknown')}")

        return f"{resource_type} {item.get('id', 'unknown')}"

    def _build_web_url(self, item: Dict[str, Any], resource_type: str) -> str:
        """Build the Commerce7 admin URL for this item."""
        item_id = item.get("id", "")
        resource_path = _RESOURCE_URL_PATHS.get(resource_type, resource_type)
        if self.tenant_id and item_id:
            return f"https://admin.platform.commerce7.com/{self.tenant_id}/{resource_path}/{item_id}"
        return ""

    def _build_permissions(
        self, item: Dict[str, Any], resource_type: str
    ) -> List[Permission]:
        """Return org-wide READER permissions for a Commerce7 record.

        Commerce7 is B2B SaaS — all data in a tenant is visible to all tenant
        admin users.  The permission object is built once in init() and reused.
        """
        return self._org_permission

    # ─── Content streaming ────────────────────────────────────────────

    async def stream_record(self, record: Record) -> StreamingResponse:
        """Stream the content of a Commerce7 record as structured JSON/Markdown."""
        if not self.commerce7_client:
            raise HTTPException(
                status_code=HttpStatusCode.SERVICE_UNAVAILABLE.value,
                detail="Commerce7 connector not initialized",
            )

        # Parse resource_type and item_id from external_record_id
        # Format: "{resource_type}/{item_id}"
        parts = record.external_record_id.split("/", 1)
        if len(parts) != 2:
            raise HTTPException(
                status_code=HttpStatusCode.BAD_REQUEST.value,
                detail=f"Invalid external_record_id format: {record.external_record_id}",
            )

        resource_type, item_id = parts

        # Fetch the individual record from Commerce7
        get_methods = {
            RESOURCE_CUSTOMERS: self.commerce7_client.get_customer,
            RESOURCE_ORDERS: self.commerce7_client.get_order,
            RESOURCE_PRODUCTS: self.commerce7_client.get_product,
            RESOURCE_CLUBS: self.commerce7_client.get_club,
        }

        get_fn = get_methods.get(resource_type)
        if get_fn:
            response = await get_fn(item_id)
            if not response.success:
                raise HTTPException(
                    status_code=HttpStatusCode.NOT_FOUND.value,
                    detail=f"Record not found: {response.error}",
                )
            content = self._format_record_as_markdown(response.data, resource_type)
        else:
            # For resource types without individual get endpoints,
            # return a formatted version of the record name
            content = f"# {record.record_name}\n\nResource type: {resource_type}\nID: {item_id}"

        return create_stream_record_response(
            content,
            filename=record.record_name,
            mime_type=record.mime_type,
            fallback_filename=f"record_{record.id}",
        )

    def _format_record_as_markdown(self, data: Dict[str, Any], resource_type: str) -> str:
        """Format a Commerce7 record as Markdown for indexing."""
        if resource_type == RESOURCE_CUSTOMERS:
            return self._format_customer_markdown(data)
        elif resource_type == RESOURCE_ORDERS:
            return self._format_order_markdown(data)
        elif resource_type == RESOURCE_PRODUCTS:
            return self._format_product_markdown(data)
        elif resource_type == RESOURCE_CLUBS:
            return self._format_club_markdown(data)
        else:
            # Generic JSON dump as fallback
            return f"# {resource_type}\n\n```json\n{json.dumps(data, indent=2, default=str)}\n```"

    def _format_customer_markdown(self, data: Dict[str, Any]) -> str:
        """Format a customer record as Markdown."""
        lines = [f"# Customer: {data.get('firstName', '')} {data.get('lastName', '')}"]
        lines.append("")

        if data.get("emails"):
            emails = ", ".join(e.get("email", "") for e in data["emails"])
            lines.append(f"Email: {emails}")

        if data.get("phones"):
            phones = ", ".join(p.get("phone", "") for p in data["phones"])
            lines.append(f"Phone: {phones}")

        if data.get("addresses"):
            for addr in data["addresses"]:
                addr_parts = [
                    addr.get("address"),
                    addr.get("address2"),
                    addr.get("city"),
                    addr.get("stateCode"),
                    addr.get("zipCode"),
                    addr.get("countryCode"),
                ]
                lines.append(f"Address: {', '.join(p for p in addr_parts if p)}")

        if data.get("tags"):
            lines.append(f"Tags: {', '.join(data['tags'])}")

        if data.get("clubMemberships"):
            memberships = [m.get("clubTitle", "") for m in data["clubMemberships"]]
            lines.append(f"Club Memberships: {', '.join(m for m in memberships if m)}")

        if data.get("birthDate"):
            lines.append(f"Birth Date: {data['birthDate']}")

        if data.get("customerGroups"):
            groups = [g.get("title", "") for g in data["customerGroups"]]
            lines.append(f"Customer Groups: {', '.join(g for g in groups if g)}")

        lines.append(f"\nCreated: {data.get('createdAt', 'N/A')}")
        lines.append(f"Updated: {data.get('updatedAt', 'N/A')}")

        return "\n".join(lines)

    def _format_order_markdown(self, data: Dict[str, Any]) -> str:
        """Format an order record as Markdown."""
        lines = [f"# Order #{data.get('orderNumber', data.get('id', 'Unknown'))}"]
        lines.append("")

        lines.append(f"Status: {data.get('orderFulfillmentStatus', 'N/A')}")
        lines.append(f"Payment Status: {data.get('orderPaidStatus', 'N/A')}")
        lines.append(f"Type: {data.get('type', 'N/A')}")

        if data.get("customer"):
            c = data["customer"]
            lines.append(f"Customer: {c.get('firstName', '')} {c.get('lastName', '')}")

        if data.get("items"):
            lines.append("\n## Order Items\n")
            for item in data["items"]:
                title = item.get("title", "Unknown")
                qty = item.get("quantity", 0)
                price = item.get("price", 0)
                lines.append(f"- {title} (x{qty}) - ${price / 100:.2f}")

        if data.get("total") is not None:
            lines.append(f"\nTotal: ${data['total'] / 100:.2f}")
        if data.get("subTotal") is not None:
            lines.append(f"Subtotal: ${data['subTotal'] / 100:.2f}")
        if data.get("taxTotal") is not None:
            lines.append(f"Tax: ${data['taxTotal'] / 100:.2f}")
        if data.get("shippingTotal") is not None:
            lines.append(f"Shipping: ${data['shippingTotal'] / 100:.2f}")

        lines.append(f"\nOrder Date: {data.get('createdAt', 'N/A')}")

        return "\n".join(lines)

    def _format_product_markdown(self, data: Dict[str, Any]) -> str:
        """Format a product record as Markdown."""
        lines = [f"# {data.get('title', 'Unknown Product')}"]
        lines.append("")

        if data.get("content"):
            lines.append(data["content"])
            lines.append("")

        if data.get("type"):
            lines.append(f"Type: {data['type']}")

        if data.get("vendorName"):
            lines.append(f"Vendor: {data['vendorName']}")

        if data.get("tags"):
            lines.append(f"Tags: {', '.join(data['tags'])}")

        if data.get("variants"):
            lines.append("\n## Variants\n")
            for variant in data["variants"]:
                title = variant.get("title", "Default")
                sku = variant.get("sku", "N/A")
                price = variant.get("price", 0)
                lines.append(f"- {title} (SKU: {sku}) - ${price / 100:.2f}")

        lines.append(f"\nCreated: {data.get('createdAt', 'N/A')}")

        return "\n".join(lines)

    def _format_club_markdown(self, data: Dict[str, Any]) -> str:
        """Format a club record as Markdown."""
        lines = [f"# {data.get('title', 'Unknown Club')}"]
        lines.append("")

        if data.get("content"):
            lines.append(data["content"])
            lines.append("")

        if data.get("clubType"):
            lines.append(f"Club Type: {data['clubType']}")

        if data.get("signUpEnabled") is not None:
            lines.append(f"Sign Up Enabled: {data['signUpEnabled']}")

        if data.get("memberCount") is not None:
            lines.append(f"Member Count: {data['memberCount']}")

        lines.append(f"\nCreated: {data.get('createdAt', 'N/A')}")

        return "\n".join(lines)

    # ─── URL & helpers ────────────────────────────────────────────────

    async def get_signed_url(self, record: Record) -> Optional[str]:
        """Commerce7 doesn't use signed URLs — return the admin panel URL."""
        return record.weburl if hasattr(record, "weburl") else None

    async def handle_webhook_notification(
        self, org_id: str, notification: Dict
    ) -> bool:
        """Handle Commerce7 webhook notifications for real-time sync.

        Commerce7 webhooks support:
          - Objects: Customer, Product, Order, Group
          - Actions: Create, Update, Delete, BulkUpdate
        """
        try:
            object_type = notification.get("objectType", "").lower()
            action = notification.get("action", "").lower()
            payload = notification.get("payload", {})

            self.logger.info(f"Received Commerce7 webhook: {action} {object_type}")

            # Map webhook object types to resource types
            webhook_to_resource = {
                "customer": RESOURCE_CUSTOMERS,
                "product": RESOURCE_PRODUCTS,
                "order": RESOURCE_ORDERS,
            }

            resource_type = webhook_to_resource.get(object_type)
            if not resource_type:
                self.logger.warning(f"Unhandled webhook object type: {object_type}")
                return True

            if action in ("create", "update", "bulkupdate"):
                # Re-sync the affected resource type
                await self._sync_resource_type(resource_type)
            elif action == "delete":
                # Handle deletion - mark records as deleted
                item_id = payload.get("id")
                if item_id:
                    self.logger.info(
                        f"Webhook delete for {object_type} {item_id} - "
                        "will be handled on next sync."
                    )

            return True

        except Exception as e:
            self.logger.error(f"Error handling Commerce7 webhook: {e}", exc_info=True)
            return False

    async def cleanup(self) -> None:
        """Release resources."""
        try:
            if self.commerce7_client:
                await self.commerce7_client.close()
                self.commerce7_client = None
        except Exception as e:
            self.logger.error(f"Error during Commerce7 cleanup: {e}", exc_info=True)

    async def reindex_records(self, records: List[Record]) -> None:
        """Reindex specific records by re-fetching from Commerce7."""
        if not self.commerce7_client:
            self.logger.error("Commerce7 client not initialized for reindex.")
            return

        for record in records:
            try:
                parts = record.external_record_id.split("/", 1)
                if len(parts) != 2:
                    continue
                resource_type, item_id = parts
                self.logger.info(f"Reindexing {resource_type}/{item_id}")
                # The next full sync will pick up changes
            except Exception as e:
                self.logger.error(f"Error reindexing record {record.id}: {e}")

    async def get_filter_options(
        self, filter_key: str, **kwargs
    ) -> Optional[Dict]:
        """Return dynamic filter options. Commerce7 uses static filters only."""
        return None

    # ─── Utility ──────────────────────────────────────────────────────

    @staticmethod
    def _parse_datetime(dt_str: Optional[str]) -> Optional[int]:
        """Parse ISO datetime string to epoch milliseconds."""
        if not dt_str:
            return None
        try:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return None
