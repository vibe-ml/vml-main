#!/usr/bin/env bash
# Run uv commands in Docker next to a disposable PostgreSQL for tests.
# Usage: scripts/dev.sh uv run python -m pytest
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd -W 2>/dev/null || pwd)
network=social-dev
database=social-test-pg
image=${UV_IMAGE:-ghcr.io/astral-sh/uv:0.12.18-python3.14-trixie-slim}
docker network inspect "$network" >/dev/null 2>&1 || docker network create "$network" >/dev/null
if [ -z "$(docker ps -q --filter "name=^${database}$")" ]; then
  docker rm -f "$database" >/dev/null 2>&1 || true
  docker run -d --name "$database" --network "$network" \
    -e POSTGRES_PASSWORD=postgres postgres:16-alpine >/dev/null
  until docker exec "$database" pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
fi
# The virtualenv and uv cache live in volumes so host files stay untouched.
MSYS_NO_PATHCONV=1 docker run --rm --network "$network" \
  -v "$root:/app" -v investments-dev-venv:/venv -v social-dev-cache:/root/.cache/uv \
  -w /app -e UV_PROJECT_ENVIRONMENT=/venv -e UV_LINK_MODE=copy \
  -e TEST_DATABASE_URL=postgresql+psycopg://postgres:postgres@$database:5432/postgres \
  "$image" "$@"
