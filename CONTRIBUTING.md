# Contributing to Komodos PipesHub Connectors

Thanks for your interest in contributing! This project welcomes new connectors, bug fixes, performance improvements, and documentation updates.

---

## Getting Started

1. Fork this repository
2. Clone your fork: `git clone https://github.com/<your-username>/komodos-pipeshub-connectors.git`
3. Set up a local PipesHub AI instance for testing: [PipesHub AI setup guide](https://github.com/pipeshub-ai/pipeshub-ai)
4. Copy the connector files you want to work on into your PipesHub install
5. Make your changes and test them

---

## Adding a New Connector

Each connector lives in its own top-level folder and mirrors the PipesHub directory structure.

### Directory layout

```
your-connector/
├── README.md                              # What it connects to, credentials needed, usage
├── docs/                                  # Optional: diagrams, full documentation
└── backend/
    └── python/
        └── app/
            ├── sources/
            │   └── client/
            │       └── your-connector/
            │           ├── __init__.py
            │           └── your_connector.py   # REST/API client
            └── connectors/
                └── sources/
                    └── your-connector/
                        ├── __init__.py
                        ├── connector.py        # Main connector class
                        └── common/
                            ├── __init__.py
                            └── apps.py         # App registration
```

### Connector checklist

- [ ] Inherits from `BaseConnector`
- [ ] Implements `init()`, `sync()`, `stream_record()`, and `cleanup()`
- [ ] Registered with the `@ConnectorBuilder` decorator in `connector.py`
- [ ] Uses `iter_all_pages()` or equivalent streaming pattern for large datasets
- [ ] Handles errors gracefully — no unhandled exceptions from API failures
- [ ] Credentials stored via PipesHub's Etcd config, not hardcoded
- [ ] README explains what the connector syncs, what credentials are needed, and how to get them
- [ ] Tested against a real instance of the external service

---

## Pull Request Guidelines

- Keep PRs focused — one connector or one fix per PR
- Include a clear description of what the connector does or what the fix addresses
- Test against a live instance before submitting
- Follow the existing code style (async/await, type hints, logging via `self.logger`)

---

## Reporting Issues

Open a GitHub issue with:
- Which connector is affected
- What you expected to happen
- What actually happened
- Relevant log output

---

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 license.
