# Developing and deploying containerized pipelines

Run local commands from the repository root unless stated otherwise.

For host placement, follow the [deployment directory layout](deployment-layout.md):
`projects/<project>`, `configs/<project>.env`, and `secrets/<project>.env` under
`/opt/dagster`. Each project owns `projects/<project>/compose.yaml`, selected by
the root `COMPOSE_FILE` list. Its relative paths use the shared deployment root.
The bundled pipeline and script examples below describe the
existing build layout; migration requires updating their paths explicitly.

## How this project runs pipelines

A Dagster job defines a DAG of operations (`op`s). Assets can also describe
data dependencies. Export jobs, assets, resources, schedules, and sensors through
the `defs` object in `dagster/code/definitions.py`.

The `pipelines` code location serves these definitions over gRPC. The code server
and run containers use the same pipeline image. `DAGSTER_CURRENT_IMAGE` tells
`DockerRunLauncher` which image to launch. Each run gets its own container;
individual operations do not get separate containers with this configuration.
See the [Dagster Docker deployment guide](https://docs.dagster.io/deployment/oss/deployment-options/docker).

The relevant files are:

- `dagster/code/`: pipeline source, copied into the image at `/opt/dagster/code`.
- `dagster/requirements-pipeline.txt`: pipeline dependencies.
- `dagster/Dockerfile.pipeline`: Python 3.12 pipeline image, running as UID 10001.
- `dagster/workspace.yaml`: registered code locations.
- `dagster/compose.yaml`: services, images, environment, and network.
- `dagster/dagster.yaml`: queue, launcher, storage, and compute logs.
- `dagster/deploy.sh`: remote transfer, image tagging, build, and startup.

## 1. Add a pipeline

For a minimal example, add these definitions to `dagster/code/definitions.py`.
Keep the existing `Definitions`, `job`, and `op` imports and smoke-test job.

```python
@op
def extract_numbers():
    return [1, 2, 3]


@op
def sum_numbers(numbers: list[int]) -> int:
    return sum(numbers)


@job
def numbers_pipeline():
    sum_numbers(extract_numbers())
```

Replace the existing `defs = ...` assignment with:

```python
defs = Definitions(jobs=[deployment_smoke_test, numbers_pipeline])
```

The output passed between operations creates the dependency. Keep one exported
`defs` object. For larger pipelines, move logic into modules under `code/` and
import the definitions into this entry point.

Add required packages to `dagster/requirements-pipeline.txt`. The current pins
are `dagster==1.13.23`, `dagster-postgres==0.29.23`, and
`dagster-docker==0.29.23`. Keep Dagster packages compatible with the infrastructure
requirements. Install required OS packages in `Dockerfile.pipeline` before
`USER dagster`.

Keep external calls out of module import: the code server imports definitions
before any run starts. Make writes safe to retry. Use resources for external
clients, and persist durable outputs in a database, object store, or explicitly
configured persistent storage. A run container's writable layer is temporary.

## 2. Test locally

Use Python 3.12 and an isolated environment:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r dagster/requirements-pipeline.txt
PYTHONPATH=dagster/code .venv/bin/python - <<'PY'
from definitions import deployment_smoke_test, numbers_pipeline

assert deployment_smoke_test.execute_in_process().success
result = numbers_pipeline.execute_in_process()
assert result.success
assert result.output_for_node("sum_numbers") == 6
PY
```

These checks use an ephemeral instance. They check pipeline logic, not production
PostgreSQL, the run queue, or Docker launching. Use test credentials and fixtures
when adding external integrations. See [Dagster testing](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops).

Build and check the actual image to catch missing dependencies and files:

```bash
docker build -f dagster/Dockerfile.pipeline -t vml-dagster-pipelines:dev dagster
docker run --rm --workdir /opt/dagster/code vml-dagster-pipelines:dev \
  python -c 'from definitions import numbers_pipeline; result = numbers_pipeline.execute_in_process(); assert result.success; assert result.output_for_node("sum_numbers") == 6'
```

`dagster/.dockerignore` allows only selected paths. If a pipeline needs files
outside `code/`, update the Dockerfile, `.dockerignore`, and the deployment
script's transfer list and revision inputs. Files in `code/` are already included.
Rebuild the image after source changes; production code is not bind-mounted.

## 3. Configure credentials and storage

The root `.env` controls SSH deployment. The remote `TARGET_DIR/.env` controls
Compose and contains the generated database password. They are different files.

For a project using the agreed layout, put non-secret settings in
`configs/<project>.env` and credentials such as `SOURCE_API_TOKEN` in
`secrets/<project>.env` on the host.

1. Load these files through `env_file` on the project's code service and the
   launching infrastructure services. Preserve existing environment files.
2. Forward `SOURCE_API_TOKEN` by name through the project's Docker container
   context, or the shared launcher's `run_launcher.config.env_vars` when appropriate.
   Preserve `DAGSTER_POSTGRES_PASSWORD` and other existing launcher settings.
3. Read the variable through resource configuration or `os.environ` inside the pipeline.
4. Recreate affected services and verify the variable reaches a launched run without
   printing its value.

Do not put credentials in source, Dockerfiles, image build arguments, or logs.
Compose `.env` values are not automatically injected into containers. Run
containers also do not inherit code-server mounts, environment, or resource limits.

Runs join `dagster_default` and mount `dagster_dagster_storage` at
`/opt/dagster/storage`. Add extra run mounts under
`run_launcher.config.container_kwargs.volumes`, preserving the existing mount.
Bind-mount sources must exist on the remote Docker host and be accessible to
UID 10001. Use `context.instance` for Dagster instance services; the launcher
passes instance configuration to runs.

The queue allows four simultaneous runs. No per-run CPU or memory limits are
configured. Adjust the launcher configuration if a workload needs limits.

## 4. Deploy

The remote host needs Docker with Compose, Python 3, and the private address
already configured. The SSH account needs noninteractive access, Docker access,
and passwordless sudo for directory setup. Configure the tunnel using
[its guide](../sshuttle/README.md).

Create or update the root `.env` with your deployment settings. Example:

```dotenv
USERNAME=ansible
HOSTNAME=vml.pub
TARGET_DIR=/opt/dagster
HOST_INT_IP=10.255.0.1
```

Use the public SSH endpoint for `HOSTNAME`; the private endpoint depends on the
tunnel. Before replacing a code location, pause its schedules and sensors and
let its queued runs drain. Existing running containers retain their image, but
queued runs and evaluations can contain stale code references.

```bash
./dagster/deploy.sh
```

The script transfers the working files, including uncommitted pipeline edits,
and builds on the remote host. No registry push is needed. It derives
`vml-dagster-pipelines:<content-hash>` from `Dockerfile.pipeline`,
`requirements-pipeline.txt`, and `code/`, then records it as `PIPELINE_IMAGE` in
the remote `.env`. Compose supplies that tag as `DAGSTER_CURRENT_IMAGE`.

The script preserves the database password and data volumes and waits for
healthy services. The hash identifies source inputs, not fully locked dependencies:
base image tags and transitive Python packages can change between builds.

If you edit `dagster.yaml` or `workspace.yaml`, restart the infrastructure services
after deployment so both processes reload their configuration. On the remote
host, from `TARGET_DIR`:

```bash
docker compose restart webserver daemon
docker compose ps
```

Pipeline-only changes do not need an infrastructure image change. When changing
infrastructure packages, update `requirements.txt` and the infrastructure image
tag in `compose.yaml` together.

## 5. Verify and enable automation

Open <http://10.255.0.1:3000> through the tunnel, or use your configured private
address. Confirm the `pipelines` code location loads and lists the new job.
Launch `deployment_smoke_test`, then launch your new job from its Launchpad.
Confirm SUCCESS, step outputs, and readable compute logs.

On the remote host, from `TARGET_DIR`:

```bash
docker compose ps
docker compose logs --tail=100 code daemon webserver
docker ps -a --filter label=deployment=vml-dagster
```

Inspect the run's `docker/container_id` and `dagster/image` tags in the UI.
The run should use the new pipeline image in a separate container. Check a
specific container with `docker logs <container-id>` and
`docker inspect <container-id>`.

Register any schedules or sensors in `Definitions(schedules=[...], sensors=[...])`
alongside the jobs. Redeploy, then enable them in the UI after a successful
manual run. The daemon must remain healthy to evaluate automation and launch
queued runs. Resume any automation paused for deployment.

## Separate dependency environments

Multiple jobs can share the current image. To isolate incompatible dependencies,
add a second code location:

1. Create a separate source directory, requirements file, and pipeline Dockerfile.
   Include compatible Dagster packages and a gRPC startup command.
2. Install the project overlay at `projects/<project>/compose.yaml` and append it
   to the root `COMPOSE_FILE` list. Add a service such as `code_analytics`, modeled
   on `code`, with its own
   build and image tag. Set `DAGSTER_CURRENT_IMAGE` to that exact tag. Keep it on
   `dagster_default`, add a gRPC health check, and publish no host ports.
3. Add this entry to the existing `load_from` list in `workspace.yaml`:

   ```yaml
   - grpc_server:
       host: code_analytics
       port: 4000
       location_name: analytics
   ```

4. Extend `deploy.sh` to transfer the new build inputs and generate a separate
   image revision/environment variable. Extend `.dockerignore` if using the
   existing build context. The current script only tags the original image.
5. Add the new healthy service to infrastructure `depends_on` entries if startup
   should wait for it. Deploy, restart webserver and daemon, and verify its runs.

Each service can use internal port 4000 because it has a different hostname.
Both locations share the instance, run queue, and storage. Their images must be
available to the remote Docker daemon; changing workspace configuration alone
does not build or distribute them.

## Troubleshooting and rollback

- **Code location fails:** check `docker compose logs code` for import errors,
  missing packages, and definition-loading failures.
- **Runs remain queued:** check daemon health and whether four runs are active.
- **Run startup fails:** check daemon logs, image availability, Docker socket
  permissions, and explicitly forwarded environment variables.
- **Files or logs are missing:** check run mounts and UID 10001 write access.

Retain old pipeline images until their runs finish. For a pipeline-only rollback,
pause automation and drain queued runs. On the remote host, set `PIPELINE_IMAGE`
in `TARGET_DIR/.env` to a retained known-good tag, then run from `TARGET_DIR`:

```bash
docker compose up -d --no-build --force-recreate --wait code
```

Verify the code location and smoke-test it before resuming automation. Do not
run `deploy.sh` during this rollback: it recalculates the tag from current source.
Restore the matching source revision before the next normal deployment. This
rolls back pipeline code only; it does not undo data writes or schema changes.

Completed run containers remain available for inspection. Remove selected
completed containers only after review. Compose stop/down does not stop the
independent run containers. Never use `docker compose down --volumes` to deploy
or roll back: it deletes instance data. See the [operations guide](../dagster/README.md)
for backups and shutdown procedures.
