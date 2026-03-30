#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy.sh — Copy the InnoVint connector into a PipesHub repo
#
# Usage:
#   ./deploy.sh /path/to/pipeshub-root
#
# What it does:
#   1. Copies the Python API client and connector packages into PipesHub's backend
#   2. Copies the Node.js Connector Manager module into PipesHub's Node.js service
#   3. Installs the aiohttp Python dependency
#   4. Reminds you of the two manual steps (ConnectorFactory patch + router registration)
#
# After running this script, rebuild your PipesHub Docker image or restart
# the Python backend and Node.js services.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Parse args ───────────────────────────────────────────────────────────────
PIPESHUB_ROOT="${1:-}"
if [[ -z "$PIPESHUB_ROOT" ]]; then
  echo "Usage: ./deploy.sh /path/to/pipeshub-root"
  echo ""
  echo "  /path/to/pipeshub-root  — The root of your PipesHub repo (the folder"
  echo "                            that contains backend/ and services/nodejs/)."
  exit 1
fi

# Resolve to absolute path
PIPESHUB_ROOT="$(cd "$PIPESHUB_ROOT" && pwd)"

# Confirm it looks like a PipesHub repo
if [[ ! -d "$PIPESHUB_ROOT/backend/python/app" ]]; then
  echo "ERROR: $PIPESHUB_ROOT does not look like a PipesHub repo."
  echo "       Expected to find: $PIPESHUB_ROOT/backend/python/app"
  exit 1
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║          InnoVint → PipesHub Connector — Deployment             ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "  Target: $PIPESHUB_ROOT"
echo ""

# ── Step 1: Python API client ─────────────────────────────────────────────────
echo "▸ [1/3] Copying Python API client (sources/external/innovint)..."
DEST_EXTERNAL="$PIPESHUB_ROOT/backend/python/app/sources/external/innovint"
mkdir -p "$DEST_EXTERNAL"
cp -r backend/python/app/sources/external/innovint/. "$DEST_EXTERNAL/"
echo "  ✓ Copied to: $DEST_EXTERNAL"

# ── Step 2: Python connector ─────────────────────────────────────────────────
echo ""
echo "▸ [2/3] Copying Python connector (connectors/sources/innovint)..."
DEST_CONNECTOR="$PIPESHUB_ROOT/backend/python/app/connectors/sources/innovint"
mkdir -p "$DEST_CONNECTOR"
cp -r backend/python/app/connectors/sources/innovint/. "$DEST_CONNECTOR/"
# Remove __pycache__ from the destination (stale bytecode from dev machine)
find "$DEST_CONNECTOR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
echo "  ✓ Copied to: $DEST_CONNECTOR"

# ── Step 3: Node.js module ───────────────────────────────────────────────────
echo ""
echo "▸ [3/3] Copying Node.js Connector Manager module..."
DEST_NODE="$PIPESHUB_ROOT/services/nodejs/apps/connector-manager/src/modules/innovint"
mkdir -p "$DEST_NODE"
cp -r services/nodejs/apps/connector-manager/src/modules/innovint/. "$DEST_NODE/"
echo "  ✓ Copied to: $DEST_NODE"

# ── Python dependency ────────────────────────────────────────────────────────
echo ""
echo "▸ Installing Python dependency (aiohttp)..."
if pip install "aiohttp>=3.9.0,<4.0.0" --quiet 2>/dev/null; then
  echo "  ✓ aiohttp installed"
else
  echo "  ⚠ pip install failed — run manually: pip install 'aiohttp>=3.9.0,<4.0.0'"
fi

# ── Manual steps reminder ────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  Files copied.  Two manual steps remain before you can deploy:  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "  STEP A — Apply the ConnectorFactory patch (Python)"
echo "  ────────────────────────────────────────────────────"
echo "  Edit: $PIPESHUB_ROOT/backend/python/app/connectors/core/factory/connector_factory.py"
echo ""
echo "    1. Add import (near the other connector imports):"
echo "       from app.connectors.sources.innovint.connector import InnovintConnector"
echo ""
echo "    2. Add to _CONNECTOR_MAP:"
echo "       \"innovint\": InnovintConnector,"
echo ""
echo "  Also edit: $PIPESHUB_ROOT/backend/python/app/config/constants/arangodb.py"
echo "    Add INNOVINT = \"innovint\" to the Connectors class"
echo "    Add INNOVINT = \"InnoVint\" to the AppGroup class"
echo ""
echo "  Full diff: backend/python/app/connectors/core/factory/connector_factory.patch.md"
echo ""
echo "  STEP B — Register the Node.js router"
echo "  ────────────────────────────────────────────────────"
echo "  In your connector manager router file (usually connectors.router.ts or app.ts):"
echo ""
echo "    import { innovintRouter } from './modules/innovint/innovint.connector';"
echo "    app.use('/api/v1/connectors/innovint', innovintRouter);"
echo ""
echo "  Then rebuild the Node.js service:"
echo "    cd $PIPESHUB_ROOT/services/nodejs && npm run build"
echo ""
echo "  STEP C — Rebuild / restart"
echo "  ────────────────────────────────────────────────────"
echo "  If using Docker:  rebuild your PipesHub Docker image and redeploy."
echo "  If running bare:  restart the PipesHub Python backend and Node.js service."
echo ""
echo "  STEP D — Verify"
echo "  ────────────────────────────────────────────────────"
echo "  Send a test event to confirm the connector is registered:"
echo "    echo '{\"event\":\"innovint.resync\",\"orgId\":\"test-org\",\"syncType\":\"full\"}' \\"
echo "      | kcat -P -b localhost:9092 -t sync-events -k test-org"
echo ""
echo "  Expect to see in logs: InnovintConnector  Starting full sync for org=test-org"
echo ""
