# OpenAlex deployment on blanco

The crawler is a separate `openalex` code location in the existing Dagster instance at <http://blanco:3002>. Deployment files live in this crawler repository; shared Dagster infrastructure remains in the sibling `vml-dagster` repository. Source and dependencies are copied into a dedicated Python 3.14 image; source files and secrets are not bind-mounted into the image.

## Configuration

- `deploy/compose.openalex.blanco.yaml`: code service, per-location run environment names, networks, resource limits, and persistent volumes.
- `deploy/workspace.openalex.yaml`: existing pipelines plus OpenAlex.
- `deploy/deploy-openalex.sh`: copies approved source/configuration, computes image tags, builds, and deploys locally to `/opt/dagster`.
- `/opt/dagster/secrets/openalex.env`: operator-provisioned `OPENALEX_DATABASE_URL` and `OPENALEX_API_KEY`, mode 0600 in a restricted directory. This file is not in Git.
- `/opt/dagster/.env`: Compose interpolation, image tags, and `COMPOSE_FILE=compose.yaml:compose.override.yaml:compose.openalex.blanco.yaml`.

Only the daemon receives OpenAlex secret values. `DAGSTER_CONTAINER_CONTEXT` on the code server advertises names for the daemon to forward into run containers. The code server also receives `DAGSTER_POSTGRES_PASSWORD` because schedule evaluation reconstructs the Dagster instance there. OpenAlex API and crawler database secrets still go only to the daemon and run containers.

Run containers join both `dagster_default` and `local-db_default`, retain the Dagster storage volume, and bind `/data/vml/dev:/data`. The application uses `/data/openalex`. Runs have a 2 GiB memory limit and two CPUs. PostgreSQL `local-db-pg` hosts `dev_vml` with the dedicated `vml_openalex` login. Dagster metadata remains in its original database/container.

## Redeploy

Stop OpenAlex schedules and let queued/running jobs finish before replacing its code. Use this deployment entrypoint for the combined deployment; the original generic script installs the original workspace without OpenAlex.

```bash
./deploy/deploy-openalex.sh
```

Run from the crawler repository. The script reads shared infrastructure from `../vml-dagster/dagster` by default; set `DAGSTER_SOURCE` to override that directory and `OPENALEX_SOURCE` to override the crawler source.

The target directory defaults to `/opt/dagster`; override with `DAGSTER_TARGET_DIR` only when configuring another local installation. The source script assumes runtime credentials are already provisioned. It preserves database volumes and secrets, and rebuilds from the current working tree, including uncommitted changes. Tags hash build inputs; they are not fully immutable registry digests because base-image tags and infrastructure transitive dependencies can change.

Verify both code locations in the UI and launch `deployment_smoke_test`. From the crawler repository, run `uv run python scripts/deployment/check_schedules.py`; it dry-runs all three schedules and requires a valid run request without launching collection. An enabled schedule alone does not prove successful tick evaluation. Then launch the required OpenAlex job. Schedules are stored in Dagster's database and survive deployment; enable them only after successful manual checks.

## Jobs and pilot

- `taxonomy_job`: all four hierarchy levels, daily at 00:00 UTC.
- `daily_refresh_job`: bounded API acquisition, daily at 00:15 UTC.
- `snapshot_job`: manually configured bounded original Parquet object.
- `rebuild_job`: one pending derived batch, every five minutes.

The daily refresh cap is 100 list requests, with account consumption considered by the existing scheduler. Collection starts inclusively on 2026-06-01 (UTC date), without domain or field restrictions. Data tables use the `raw` and `tmd` schemas with the `openalex_` prefix; migration state is `migrations.alembic_openalex`. The budget-fairness implementation is under repair in crawler ticket 07; deployment does not certify its fairness or full allowance behavior. The original deployment retained 4,498 works from 45 pages in the retired database; these are historical validation results, not counts for `dev_vml`. Full bootstrap is not exposed.

The approved pilot lasts at least 72 hours. Its exact deadline is recorded in `/opt/dagster/pilot.env`. The `pilot-openalex.timer` systemd timer runs every fifteen minutes, writes evidence under `/opt/dagster/validation`, and stops all three OpenAlex schedules once the deadline is reached. It then disables itself. It leaves data and running jobs intact. Check timer and service status with:

```bash
systemctl status pilot-openalex.timer
journalctl -u pilot-openalex.service
```

The observer is copied from `deploy/pilot-observe-openalex.sh`; its measurement program comes from `vml-crawler-openalex/scripts/deployment/measure.py`. The host Python executable and deadline are explicit in `pilot.env`. The timer/service source files are checked in alongside the deployment script. Installing or restarting this pilot is separate from ordinary redeployment and must not silently reset its deadline.

For an immediate manual measurement, run the measurement program with permission to read runtime credentials and data. It emits counts, sizes, checksums, run states, and allowance facts, not passwords or work payloads. Measurements can overlap active jobs, so record their timestamps and use a settled snapshot for final comparisons.

## Rollback

Pre-deployment configuration and a Dagster metadata SQL dump are retained under `/opt/dagster/backups/20260924T112730Z`. Old images remain available. Stop the pilot timer and OpenAlex schedules, drain jobs, then restore the matching Compose/workspace configuration and image references. Recreate affected services. Preserve `local-db-pg`, collection data, and all shared volumes. Image rollback does not undo collection migrations or imported data.

## September 24 redeployment

Task `deploy-02` replaces the old database with a fresh `dev_vml` database owned by `vml_openalex`, while retaining the original data files. The old database is backed up under `/opt/dagster/backups/deploy-02-20260924T155905Z/dev_vml_openalex.dump`. Its restore listing and previous configuration are stored alongside it; credentials remain restricted. The database was dropped only after jobs were drained and its dump validated.

Use `OPENALEX_RUN_REQUEST_LIMIT=350` only for the bounded redeployment check, since earlier account usage plus taxonomy collection already exceeds the ordinary cap. Ordinary Compose invocations default to 100. The code service explicitly keeps the processing schedule at five-minute intervals.

The observer now reads the current SQLAlchemy schema and records the database, migration revision, frozen scopes, row counts, and file checksums. The pilot retains its original September 27 deadline. Database replacement and the changed collection scope break continuity with the original pilot; report the two segments separately. Existing files are retained, so filesystem totals include artifacts from both deployments.
