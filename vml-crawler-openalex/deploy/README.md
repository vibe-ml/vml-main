# OpenAlex deployment files

Use the Compose overlay for the target host with the shared Dagster configuration from `vml-dagster`.

| File                                                         | Purpose                                                                  | Required for               |
| ------------------------------------------------------------ | ------------------------------------------------------------------------ | -------------------------- |
| [compose.openalex.blanco.yaml](compose.openalex.blanco.yaml) | Blanco service, storage, network, and schedule configuration.            | Blanco deployment          |
| [compose.openalex.vml.yaml](compose.openalex.vml.yaml)       | VML service, storage, network, and schedule configuration.               | VML deployment             |
| [workspace.openalex.yaml](workspace.openalex.yaml)           | Registers OpenAlex alongside existing Dagster pipelines.                 | Both hosts                 |
| [deploy-openalex.sh](deploy-openalex.sh)                     | Stages source, builds images, and restarts the Blanco deployment.        | Optional Blanco automation |
| [pilot-observe-openalex.sh](pilot-observe-openalex.sh)       | Records measurements and stops OpenAlex schedules at the pilot deadline. | Optional bounded pilot     |
| [pilot-openalex.service](pilot-openalex.service)             | Runs the pilot observer through systemd.                                 | Optional bounded pilot     |
| [pilot-openalex.timer](pilot-openalex.timer)                 | Triggers the observer every 15 minutes.                                  | Optional bounded pilot     |

See the [Blanco guide](../docs/deploy/openalex-deployment-blanco.md) or [VML guide](../docs/deploy/openalex-deployment-vml.md) for deployment instructions.

Renaming these source files does not update installed host copies. When migrating an existing pilot, disable the old `openalex-pilot.timer` before installing the renamed units; preserve the existing pilot deadline.

## VML host layout

The agreed OpenAlex paths are `/opt/dagster/projects/vml-crawler-openalex` for
build inputs and the installed `compose.yaml`, `/opt/dagster/configs/vml-crawler-openalex.env` for non-secret runtime
settings, and `/opt/dagster/secrets/vml-crawler-openalex.env` for credentials.
Install `deploy/compose.openalex.vml.yaml` as
`/opt/dagster/projects/vml-crawler-openalex/compose.yaml`. Shared Compose files stay
at `/opt/dagster`; data stays at `/data/demo/openalex`. The root `.env` selects:

```dotenv
COMPOSE_FILE=compose.yaml:compose.override.yaml:projects/vml-crawler-openalex/compose.yaml
```

Run Compose from `/opt/dagster`. Relative build, environment-file, and bind-mount
paths in the project overlay resolve against the shared root Compose file.

The VML overlay uses these paths and forwards runtime settings by name to run
containers. Install `config.openalex.vml.env` as the non-secret config file.
The publication boundary is January 1, 2024 (00:00 UTC). See the
[VML migration procedure](../docs/deploy/openalex-deployment-vml.md#migrate-configuration-and-project-paths)
for existing installations. Blanco retains its separate deployment configuration.
