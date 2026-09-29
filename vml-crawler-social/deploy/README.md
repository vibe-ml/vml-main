# Deployment

The crawler is a Dagster code location in the shared instance from [vml-dagster](../../vml-dagster), next to `openalex`. It follows the project layout of [deployment-layout.md](../../vml-dagster/docs/deployment-layout.md).

| Host path | Content |
| --- | --- |
| `/opt/dagster/projects/vml-crawler-social/` | `Dockerfile`, `pyproject.toml`, `uv.lock`, `alembic.ini`, `src/`, and [compose.social.vml.yaml](compose.social.vml.yaml) as `compose.yaml` |
| `/opt/dagster/configs/vml-crawler-social.env` | `SOCIAL_*` settings from [.env.example](../.env.example) without secrets |
| `/opt/dagster/secrets/vml-crawler-social.env` (0600) | `SOCIAL_DATABASE_URL`, `SOCIAL_STACKEXCHANGE_KEY`, `SOCIAL_BLUESKY_APP_PASSWORD` |

Local deployment files are `deploy/vml-crawler-social.configs.env` and
`deploy/vml-crawler-social.secrets.env` (both Git-ignored). Copy them to the
corresponding `configs/` and `secrets/` paths above.

Dagster requires every variable listed in the overlay's `env_vars` to exist in
the launching services. Set unused `SOCIAL_STACKEXCHANGE_KEY`,
`SOCIAL_BLUESKY_HANDLE`, and `SOCIAL_BLUESKY_APP_PASSWORD` to empty values rather
than omitting them. Keep `SOCIAL_TOPIC_IDS=[]` and `SOCIAL_EXCLUDED_TERMS=[]` when
no overrides are needed.

Steps:

1. Copy the files above; set `SOCIAL_IMAGE=vml-social:<content-hash>` in `/opt/dagster/.env`.
2. Append `projects/vml-crawler-social/compose.yaml` to `COMPOSE_FILE` in `/opt/dagster/.env`.
3. Add the location to `/opt/dagster/workspace.yaml`:
   ```yaml
   - grpc_server:
       host: code_social
       port: 4000
       location_name: social
   ```
4. `docker compose build && docker compose up -d --wait && docker compose restart webserver daemon`.

Use the same database as vml-crawler-openalex. The database user needs `CREATE` on the database (for the `social` schema) and `SELECT` on the OpenAlex `raw` schema.
