#!/usr/bin/env bash
# Deploy the approved blanco configuration; secrets are provisioned separately.
set -euo pipefail
source_dir=$(cd "$(dirname "$0")" && pwd)
crawler_dir=${OPENALEX_SOURCE:-"$source_dir/.."}
dagster_dir=${DAGSTER_SOURCE:-"$source_dir/../../vml-dagster/dagster"}
target_dir=${DAGSTER_TARGET_DIR:-/opt/dagster}
test -f "$target_dir/secrets/openalex.env"
test -f "$target_dir/.env"
test -f "$crawler_dir/uv.lock"
test -f "$dagster_dir/compose.yaml"
test -d "$dagster_dir/code"
mkdir -p "$target_dir/openalex"
for file in Dockerfile .dockerignore pyproject.toml uv.lock alembic.ini; do
  cp "$crawler_dir/$file" "$target_dir/openalex/$file"
done
# A fresh source snapshot avoids retaining removed modules between deployments.
staging=$(mktemp -d "$target_dir/openalex/source.XXXXXX")
trap 'rm -rf "$staging"' EXIT
cp -a "$crawler_dir/src" "$staging/src"
if [ -d "$target_dir/openalex/src" ]; then
  mv "$target_dir/openalex/src" "$staging/previous-src"
fi
mv "$staging/src" "$target_dir/openalex/src"
for file in compose.yaml compose.override.yaml Dockerfile Dockerfile.pipeline requirements.txt requirements-pipeline.txt dagster.yaml .dockerignore; do
  if [ -f "$dagster_dir/$file" ]; then
    cp "$dagster_dir/$file" "$target_dir/$file"
  fi
done
cp "$source_dir/compose.openalex.blanco.yaml" "$target_dir/compose.openalex.blanco.yaml"
cp "$source_dir/workspace.openalex.yaml" "$target_dir/workspace.yaml"
cp -a "$dagster_dir/code/." "$target_dir/code/"
cd "$target_dir"
openalex_revision=$(tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner --exclude=__pycache__ --exclude='*.pyc' -cf - -C openalex Dockerfile .dockerignore pyproject.toml uv.lock alembic.ini src | sha256sum | cut -c1-16)
pipeline_revision=$(tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner -cf - Dockerfile.pipeline requirements-pipeline.txt code | sha256sum | cut -c1-16)
for setting in "OPENALEX_IMAGE=vml-openalex:$openalex_revision" "PIPELINE_IMAGE=vml-dagster-pipelines:$pipeline_revision" "COMPOSE_FILE=compose.yaml:compose.override.yaml:compose.openalex.blanco.yaml"; do
  key=${setting%%=*}
  if grep -q "^$key=" .env; then
    sed -i "s|^$key=.*|$setting|" .env
  else
    printf '%s\n' "$setting" >> .env
  fi
done
chmod 600 .env
docker compose config --quiet
docker compose build
docker compose up -d --no-build --wait --wait-timeout 240
# Bind-mounted instance/workspace files also need process reloads.
docker compose restart webserver daemon
docker compose ps
