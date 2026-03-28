# Commerce7 Connector for PipesHub AI

Syncs data from [Commerce7](https://commerce7.com) — a modern DTC commerce platform for wineries — into PipesHub AI for search and AI-powered insights.

---

## What It Syncs

| Resource | Description |
|----------|-------------|
| Customers | Full customer profiles including contact details and metadata |
| Orders | Order history with line items and status |
| Products | Product catalog with variants and pricing |
| Clubs | Wine club definitions |
| Club Memberships | Customer club membership records |
| Reservations | Tasting room and event reservations |
| Inventory | Product inventory levels |

---

## Prerequisites

- PipesHub AI instance running ([setup guide](https://github.com/pipeshub-ai/pipeshub-ai))
- Commerce7 account with API access
- Commerce7 App credentials (App ID + App Secret Key)

---

## Getting Your Commerce7 Credentials

1. Log into your Commerce7 admin panel
2. Go to **Settings → Developer → Apps**
3. Click **Create App**
4. Give it a name (e.g. `PipesHub AI`)
5. Select the scopes you need: Customers, Orders, Products, Clubs, Reservations, Inventory
6. Save — you'll receive an **App ID** and **App Secret Key**
7. Your **Tenant ID** is the subdomain in your admin URL (e.g. if your URL is `komodos.platform.commerce7.com`, your tenant ID is `komodos`)

---

## Installation

1. Copy the connector files into your PipesHub AI installation:

   ```bash
   cp -r backend/ /path/to/pipeshub-ai/backend/
   ```

2. Register the connector in `connector_factory.py`:

   ```python
   from app.connectors.sources.commerce7.connector import Commerce7Connector

   # Add to the connectors dict:
   "commerce7": Commerce7Connector,
   ```

3. Rebuild and restart your Docker stack:

   ```bash
   docker compose -f deployment/docker-compose/docker-compose.dev.yml build --no-cache pipeshub-ai
   docker compose -f deployment/docker-compose/docker-compose.dev.yml up -d
   ```

4. In the PipesHub UI, go to **Connectors → Commerce7**, enter your credentials, and trigger a sync.

---

## Configuration

| Field | Description |
|-------|-------------|
| App ID | Your Commerce7 app ID |
| App Secret Key | Your Commerce7 app secret (stored encrypted) |
| Tenant ID | Your Commerce7 subdomain |

---

## Architecture

The connector uses cursor-based pagination (no rate limit) and streams records 500 at a time via an async generator, keeping memory flat regardless of dataset size. See [docs/commerce7_architecture.html](./docs/commerce7_architecture.html) for the full architecture diagram and [docs/Commerce7_Connector_Documentation.docx](./docs/Commerce7_Connector_Documentation.docx) for complete technical documentation.

---

## Sync Schedule

Full sync runs every 15 minutes. Records are streamed page-by-page (500/page) and upserted immediately — each page is searchable as soon as it's processed.

---

## License

Apache 2.0 — see [LICENSE](../LICENSE).
