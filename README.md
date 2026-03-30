# Komodos PipesHub Connectors

Community connectors for [PipesHub AI](https://github.com/pipeshub-ai/pipeshub-ai) built and maintained by [Komodos](https://komodos.com).

These connectors extend PipesHub AI with integrations tailored for the wine and beverage industry, and are open-sourced for the broader PipesHub community to use and contribute to.

---

## Available Connectors

| Connector | Description | Status |
|-----------|-------------|--------|
| [Commerce7](./commerce7) | Sync customers, orders, products, clubs, memberships, reservations, and inventory from Commerce7 | ✅ Production |
| [InnoVint](./innovint) | Sync lots, vessels, lab analyses, work orders, case goods, and cost records from InnoVint | ✅ Production |

---

## Installation

Each connector mirrors the PipesHub AI directory structure so installation is straightforward. The InnoVint connector also includes a `deploy.sh` script that automates the copy steps.

### Prerequisites

- A running PipesHub AI instance ([setup guide](https://github.com/pipeshub-ai/pipeshub-ai))
- Python 3.10+
- The connector's required credentials (see each connector's README)

### Steps

1. **Copy the connector files** into your PipesHub AI installation:

   ```bash
   # Commerce7
   cp -r commerce7/backend/ /path/to/pipeshub-ai/backend/

   # InnoVint (or use the included deploy.sh for a guided install)
   ./innovint/deploy.sh /path/to/pipeshub-ai
   ```

2. **Register the connector** in `connector_factory.py`:

   ```python
   # backend/python/app/connectors/core/factory/connector_factory.py
   from app.connectors.sources.commerce7.connector import Commerce7Connector
   from app.connectors.sources.innovint.connector import InnovintConnector

   # Add to the connectors dict:
   "commerce7": Commerce7Connector,
   "innovint":  InnovintConnector,
   ```

3. **Rebuild your Docker image**:

   ```bash
   docker compose -f deployment/docker-compose/docker-compose.dev.yml build --no-cache pipeshub-ai
   docker compose -f deployment/docker-compose/docker-compose.dev.yml up -d
   ```

4. **Configure the connector** in the PipesHub UI (or via the admin API) and trigger a sync.

---

## Repository Structure

```
komodos-pipeshub-connectors/
├── commerce7/                        # Commerce7 connector
│   ├── README.md                     # Connector-specific docs
│   ├── docs/                         # Architecture diagrams and full documentation
│   └── backend/python/app/           # Drop-in files (mirrors PipesHub structure)
│       ├── sources/client/commerce7/ # REST API client
│       └── connectors/sources/commerce7/ # Main connector class
├── innovint/                         # InnoVint connector
│   ├── README.md                     # Connector-specific docs
│   ├── deploy.sh                     # One-command installer script
│   ├── docs/                         # Full technical documentation (.docx)
│   └── backend/python/app/           # Drop-in files (mirrors PipesHub structure)
│       ├── sources/external/innovint/ # Async HTTP client (InnovintClient)
│       └── connectors/sources/innovint/ # Main connector class + tests
└── ...                               # Future connectors follow the same pattern
```

---

## Contributing

We welcome contributions — new connectors, bug fixes, and improvements. See [CONTRIBUTING.md](./CONTRIBUTING.md) to get started.

---

## License

Apache 2.0 — see [LICENSE](./LICENSE). This project is built on top of [PipesHub AI](https://github.com/pipeshub-ai/pipeshub-ai), which is also Apache 2.0.

---

## About Komodos

[Komodos](https://komodos.com) builds AI-powered tools for the wine and beverage industry. These connectors were created to connect PipesHub AI with the platforms our customers use every day.
