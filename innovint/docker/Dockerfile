# ─────────────────────────────────────────────────────────────────────────────
# InnoVint Connector — Development / CI Dockerfile
#
# NOTE: This Dockerfile is for local development and CI testing only.
#
# PRODUCTION DEPLOYMENT: The InnoVint connector runs inside PipesHub's main
# Python backend via the ConnectorFactory — it is NOT a standalone service.
# Use deploy.sh to copy connector files into the PipesHub repo and rebuild
# PipesHub's own Docker image.  See the Setup & Usage Guide for details.
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

WORKDIR /app

# System deps for aiohttp
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install connector dependency
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy connector source
COPY backend/python/app/sources/external/innovint/ \
     /app/app/sources/external/innovint/
COPY backend/python/app/connectors/sources/innovint/ \
     /app/app/connectors/sources/innovint/

# Run tests (default CMD — override for other uses)
ENV PYTHONPATH=/app
CMD ["python", "-m", "pytest", "app/connectors/sources/innovint/test.py", "-v"]
