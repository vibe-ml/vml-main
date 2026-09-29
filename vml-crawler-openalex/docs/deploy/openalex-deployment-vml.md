# OpenAlex deployment on vml

The `openalex` code location runs beside `pipelines` in `/opt/dagster` on `vml.pub`. The UI is <http://vml:3000>.

## Configuration

Use `deploy/compose.openalex.vml.yaml` as the source template for the host file
`/opt/dagster/projects/vml-crawler-openalex/compose.yaml`. Use
`deploy/workspace.openalex.yaml` for the shared workspace registration. The Blanco
overlay is separate and must not be applied on VML.

The agreed host layout is:

```text
/opt/dagster/
├── .env
├── compose.yaml                     # Shared Dagster services
├── compose.override.yaml            # Shared host overrides
├── projects/vml-crawler-openalex/
│   ├── compose.yaml                 # OpenAlex service overlay
│   ├── Dockerfile
│   └── src/
├── configs/vml-crawler-openalex.env    # Non-secret runtime settings
└── secrets/vml-crawler-openalex.env   # Database URL and API key
```

This layout is deployed on VML. The checked-in Compose overlay uses these paths.
Install `deploy/config.openalex.vml.env` as the non-secret config file. The migration
procedure below documents conversion from the previous layout.

The root `.env` selects
`COMPOSE_FILE=compose.yaml:compose.override.yaml:projects/vml-crawler-openalex/compose.yaml` and the
immutable `OPENALEX_IMAGE` tag. Keep pipeline runtime values in
`configs/vml-crawler-openalex.env`, with the agreed new publication boundary:

```dotenv
OPENALEX_STORAGE_ROOT=/data/openalex
OPENALEX_PUBLICATION_FROM=2024-01-01
OPENALEX_API_REQUEST_LIMIT=100
OPENALEX_INGESTION_CADENCE=15 0 * * *
OPENALEX_TAXONOMY_CADENCE=0 0 * * *
OPENALEX_BATCH_CADENCE=*/5 * * * *
```

Run Compose from `/opt/dagster` as one shared deployment. All relative paths in
the merged files resolve against the first file, `/opt/dagster/compose.yaml`.
The nested overlay must therefore use `build.context: ./projects/vml-crawler-openalex`
and `env_file` paths beginning with `./configs/` and `./secrets/`, not paths relative
to its own directory. Preserve the existing Compose project name and service names.
See [Docker's merge rules](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/).

Leave `OPENALEX_PUBLICATION_THROUGH` unset for the runtime UTC date. There are no
domain or field restrictions. Store `OPENALEX_DATABASE_URL` and `OPENALEX_API_KEY`
in `secrets/vml-crawler-openalex.env`, mode `0600` in a mode-`0700` directory owned
by the deployment account. Do not commit or copy credentials into the image.

The crawler uses `demo_vml` on `pg-postgres-1:5432` through `pg_default`.
PostgreSQL retains its existing volume on the root filesystem. The host mount
remains `/data/demo:/data`; crawler files remain under `/data/openalex` in containers.
Host project relocation does not change the image's `/opt/openalex` working directory.
Run containers keep two CPUs, a 2 GiB memory limit, and the Dagster metadata mount.

## Migrate configuration and project paths

1. Back up the deployed Compose and environment files. Pause OpenAlex automation
   and coordinate queued runs before replacing services. Retain existing source
   and credentials until validation completes.
2. Stage the Dockerfile, `.dockerignore`, `pyproject.toml`, `uv.lock`, `alembic.ini`,
   and `src` under `/opt/dagster/projects/vml-crawler-openalex`. Change
   `code_openalex.build.context` to `./projects/vml-crawler-openalex` in both the
   repository overlay and its deployed copy. Install the overlay at
   `projects/vml-crawler-openalex/compose.yaml`, replacing the old
   `compose.openalex.vml.yaml` entry in root `COMPOSE_FILE` with the nested path.
   Preserve other overlays and their order; do not select both old and new copies.
   Update source-transfer commands to preserve the installed Compose file when
   refreshing build inputs.
3. Create the config and secrets files above. On `code_openalex`, `daemon`, and
   `webserver`, load them with the following service-level entries, preserving
   any existing environment files:

   ```yaml
   env_file:
     - ./configs/vml-crawler-openalex.env
     - ./secrets/vml-crawler-openalex.env
   ```

4. Remove the explicit OpenAlex cadence values from `code_openalex.environment`
   so they do not override the config file. In `DAGSTER_CONTAINER_CONTEXT`, replace
   the inline OpenAlex values with names:

   ```json
   "env_vars": [
     "OPENALEX_DATABASE_URL", "OPENALEX_API_KEY", "OPENALEX_STORAGE_ROOT",
     "OPENALEX_API_REQUEST_LIMIT", "OPENALEX_PUBLICATION_FROM"
   ]
   ```

   Preserve networks, resource limits, storage mounts, `DAGSTER_CURRENT_IMAGE`, and
   the shared `DAGSTER_POSTGRES_PASSWORD` environment. The launching infrastructure
   services need the named variables too: run containers do not inherit the code
   service's environment automatically. Remove the obsolete
   `OPENALEX_RUN_REQUEST_LIMIT` setting from root `.env` once nothing references it.
   Service `env_file` does not provide Compose `${...}` interpolation values.
5. Validate and apply the configuration from `/opt/dagster`:

   ```bash
   docker compose config --quiet
   docker compose up -d --no-build --force-recreate code_openalex daemon webserver
   ```

   Keep the existing image for a paths/environment-only change. Rebuild with a new
   immutable tag if source or dependencies also change. Reload `openalex` in the UI.
6. Verify code-location health, schedule evaluation, and a launched run's non-secret
   settings, mounts, and database connectivity. Do not print full environments or
   resolved Compose output containing credentials. Resume automation after checks.

The publication-date change requires a new scope. Existing scans remain frozen at
2026-01-01 even after services receive the 2024 setting. If the old bootstrap is
running, terminate it through Dagster and wait for it to stop before launching
`bootstrap_job` with:

```yaml
ops:
  collect_bootstrap:
    config:
      chunk_rows: 1000
      scan_id: ""
```

Do not supply the old scan ID for the expanded scope. For an unchanged release
manifest, acquired source files are reused, but selection is processed for the new
scope. Verify its recorded start date is 2024-01-01. Future daily refresh runs use
the new policy; existing run containers keep their original settings.

## Jobs and schedules

`bootstrap_job` is exported for manual launch through Dagster. It downloads the complete manifest inventory before selecting works by publication date. Do not confuse `snapshot_job`, a bounded validation job, with the full bootstrap.

Taxonomy runs at 00:00 UTC, API refresh at 00:15 UTC, and `rebuild_job` runs every five minutes. All three schedules were enabled after successful dry-run evaluation. The full bootstrap is a separately launched, long-running job, not a recurring schedule.

Existing receipts and committed chunks support bootstrap resumption after failure. Inspect the recorded scan and release before relaunching; use the original `scan_id` in `collect_bootstrap` configuration to preserve its frozen scope. A new release or scope is not equivalent to resuming the same baseline.

## Operations

From `/opt/dagster` on vml, use `docker compose ps` to inspect services. Monitor the full bootstrap in Dagster and inspect `/data/demo/openalex/raw/snapshot/inventory` for durable Parquet files and receipts. Check both `df -h /` and `df -h /data`: database growth uses the root filesystem, while raw and derived Parquet use `/data`.

For future image updates, stage only the crawler's Dockerfile, `.dockerignore`, lockfile, project metadata, `alembic.ini`, and `src` under `/opt/dagster/projects/vml-crawler-openalex`. Build with a new immutable image tag, update `OPENALEX_IMAGE`, and apply the VML overlay at `projects/vml-crawler-openalex/compose.yaml` from the deployment root. Reload the code location in Dagster after code-server replacement. Instance or workspace changes require restarting the webserver and daemon. Avoid restarting active run containers.

The original target configuration is retained under `/opt/dagster/backups/deploy-03`. Restoring configuration does not reverse database migrations or delete collected data. Keep existing images and storage when rolling back.
