# Deployment directory layout

Use `/opt/dagster` as the shared deployment root. This is the project's host
layout convention, not a directory structure required by Dagster. Code locations
are registered explicitly in `workspace.yaml`; directory names do not register them.

```text
/opt/dagster/
├── compose.yaml
├── compose.override.yaml
├── dagster.yaml
├── workspace.yaml
├── .env
├── projects/
│   └── <project>/
│       ├── compose.yaml           # Project service overlay
│       ├── Dockerfile
│       └── src/
├── configs/
│   └── <project>.env              # Non-secret runtime configuration
├── secrets/
│   └── <project>.env              # Runtime credentials; never committed
└── backups/
```

Keep shared service definitions, instance configuration, workspace registration,
and Compose interpolation settings at the deployment root. Keep each pipeline
project's Compose overlay and build inputs under `projects/<project>`. Build immutable, versioned
images; source on the host is a build input, not a production source bind mount.
Container paths can differ from host paths.

Keep non-secret pipeline settings in `configs/<project>.env` and credentials in
`secrets/<project>.env`. Use the same project name for all three paths. Commit
source, deployment templates, and non-secret configuration examples to their
own repositories; deploy reviewed copies to the host. Restrict the secrets
directory to mode `0700` and credential files to `0600`, owned by the deployment
account. Never include secrets in image build contexts, logs, or Git.

## Compose file selection and relative paths

Use one shared Compose deployment, running commands from `/opt/dagster`. Select
project overlays in the root `.env`, after the shared base and host override:

```dotenv
COMPOSE_FILE=compose.yaml:compose.override.yaml:projects/vml-crawler-openalex/compose.yaml
```

Append other project overlays to this colon-separated list on the Linux host.
Preserve the existing Compose project name (`dagster`) and shared service names.
Do not launch a project overlay as a separate Compose stack.

With this merged-file setup, paths resolve relative to the first Compose file,
`/opt/dagster/compose.yaml`, including paths written inside project overlays:

```yaml
services:
  code_openalex:
    build:
      context: ./projects/vml-crawler-openalex
    env_file:
      - ./configs/vml-crawler-openalex.env
      - ./secrets/vml-crawler-openalex.env
```

The build context is not `.` and the environment paths do not start with `../../`.
This convention uses `COMPOSE_FILE` merging, not Compose `include`, which has
different path semantics. See [Docker's merge rules](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/).

## Environment propagation

A service's `env_file` can load its project configuration followed by its secrets.
Preserve other services' existing environment files when adding a project.
Remove duplicate explicit `environment` values that would override these files.
The root `.env` supplies Compose interpolation; service `env_file` values do not
supply `${...}` interpolation in Compose YAML.

Run containers are separate from code servers. Explicitly forward the required
variable names through the run launcher or the code location's Docker container
context. Supply those variables to the launching services (daemon and webserver
when applicable), as well as the code server. Preserve shared Dagster credentials,
networks, and storage mounts. Verify both a launched run and schedule evaluation.

## OpenAlex on VML

Use these exact host paths:

- `/opt/dagster/projects/vml-crawler-openalex`
- `/opt/dagster/configs/vml-crawler-openalex.env`
- `/opt/dagster/secrets/vml-crawler-openalex.env`

Its Compose overlay is `/opt/dagster/projects/vml-crawler-openalex/compose.yaml`.
Deploy it from the crawler repository's `deploy/compose.openalex.vml.yaml` template;
the repository filename need not match the installed filename. Its code location
remains `openalex`. Application data remains under `/data/demo/openalex`;
it does not belong in the deployment or source directories.

## BERTopic on VML

The processor overlay reads run-container `extra_hosts` from Compose
interpolation in `/opt/dagster/.env`:

```dotenv
BERTOPIC_EXTRA_HOSTS=["blanco:10.255.2.2","pdc-lite:10.255.2.6"]
```

Service `env_file` entries do not supply this value. The processor repository
owns the overlay.

## Migration status and procedure

This document defines the target convention. The existing deployment script and
Compose templates have not been migrated by this documentation change. The bundled
`pipelines` image still uses the existing root build context and `code/` inputs.
Do not assume running `dagster/deploy.sh` installs the new layout.

Before migrating a project, back up its configuration, coordinate queued runs and
automation, stage its build inputs under `projects/<project>`, and update the
transfer script and Compose build context together. Install its overlay as
`projects/<project>/compose.yaml` and replace its old root-level entry in
`COMPOSE_FILE`, preserving other overlays and their order. Split its settings into the
two environment files, update service references and run-variable forwarding,
then validate Compose without printing resolved secrets. Recreate affected services
and reload the code location. Restart infrastructure processes when changing
workspace or instance configuration. Verify a run and schedule evaluation before
resuming automation. Retain old files and images until verification completes.

Existing running containers retain their environment and image. Moving host build
inputs does not change paths inside existing images or migrate persisted data.
The generated shared Dagster database password currently remains in the protected
root `.env`; moving it is a separate infrastructure change.
