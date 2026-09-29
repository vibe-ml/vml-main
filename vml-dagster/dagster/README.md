# Dagster on VML

Open **http://10.255.0.1:3000** through the existing `sshuttle-vml.service`.
The UI binds only to `HOST_INT_IP`. PostgreSQL, code servers, and run containers
publish no host ports. Access is controlled by the private network and SSH;
the UI has no application login.

## Host directory convention

Use `/opt/dagster/projects/<project>` for pipeline build inputs and its `compose.yaml`,
`/opt/dagster/configs/<project>.env` for non-secret runtime settings, and
`/opt/dagster/secrets/<project>.env` for credentials. Shared Compose, workspace,
and instance files remain at `/opt/dagster`. Select project overlays through the
root `.env` `COMPOSE_FILE` list and run Compose from `/opt/dagster`; relative
paths in all merged files resolve against the root base file.

See [deployment directory layout](../docs/deployment-layout.md) for environment
propagation, the OpenAlex paths, and migration steps. This is the target layout;
the current deployment script still uses the legacy build paths described below.

## Images and execution

- `Dockerfile` / `requirements.txt`: infrastructure image
  `vml-dagster-infra:1.13.23`, used by the webserver and daemon. No pipeline code.
- `Dockerfile.pipeline` / `requirements-pipeline.txt` / `code/`: pipeline image
  `vml-dagster-pipelines:<content-hash>`, used by the gRPC code server and each run.

`DockerRunLauncher` starts one independent container per run using the code
server's `DAGSTER_CURRENT_IMAGE`. The queue permits four concurrent runs.
Run containers join `dagster_default`, receive `DAGSTER_POSTGRES_PASSWORD`,
and mount `dagster_dagster_storage` at `/opt/dagster/storage` for compute logs
and local artifacts. Instance configuration is passed to runs by Dagster.
Use the execution context's instance when accessing instance services in jobs.
PostgreSQL stores run/event/schedule metadata.

Only the webserver and daemon mount the host Docker socket. They run as UID
10001 with the socket's group added by Compose. Docker socket access grants
host-level control; the code server and run containers do not receive it.
No per-run CPU or memory limits are currently imposed.

## Deploy

From the repository root:

```bash
./dagster/deploy.sh
```

The script reads `USERNAME`, `HOSTNAME`, `TARGET_DIR`, and `HOST_INT_IP` from
root `.env`, checks the private address, copies files, builds images on the
remote host, and waits for healthy services. The remote host requires Docker
with Compose, Python 3, and the private address already configured. The SSH
account requires Docker access and passwordless sudo for directory setup.

A database password is generated only on first deployment and retained in
`TARGET_DIR/.env` (mode `0600`). Deployments preserve both data volumes.
The script records the Docker socket group and a pipeline release tag derived
from the pipeline Dockerfile, requirements, and code. Python and PostgreSQL
base tags and transitive Python dependencies are not locked; the release tag
identifies source content, not a fully reproducible dependency resolution.

Configuration files are mounted into infrastructure containers, so edits do
not require baking a new infrastructure image. Pipeline changes invalidate
only the pipeline image build. Infrastructure package changes require updating
its requirements and image tag in Compose. Keep Dagster versions compatible
across both images.

## Deploy a pipeline change

1. Add assets/jobs to `code/definitions.py` and dependencies to
   `requirements-pipeline.txt`.
2. Run `./dagster/deploy.sh`. The code server advertises the new image tag.
3. Confirm the `pipelines` code location loads, then launch the job in the UI.

Multiple jobs can share one code location. For a separate dependency environment,
add another pipeline image/code-server service and register it in `workspace.yaml`.
Pass additional run credentials explicitly via the launcher `env_vars` list and
supply them to the infrastructure services. Run containers do not inherit all
Compose settings automatically.

Existing run containers are independent of the code server. Coordinate deployments
with queued runs and schedule evaluations; old code references can become stale
when a code location is replaced. Retain old images until their runs are finished.

## Operate

On the remote host, from `TARGET_DIR` (currently `/opt/dagster`):

```bash
docker compose ps
docker compose logs --tail=100 webserver daemon code
docker ps -a --filter label=deployment=vml-dagster
docker compose stop
```

Completed run containers are retained for inspection. Remove selected completed
containers with `docker rm <container-id>` after reviewing them; do not remove
active runs. Compose stop/down controls the long-running services, not independent
run containers. Terminate active runs through Dagster before shutting everything down.

Back up PostgreSQL with `pg_dump`, the shared storage volume, and remote `.env`.
Do not use `docker compose down --volumes` unless intentionally deleting instance
data. Follow Dagster migration guidance before version upgrades.
Services have restart policies; host reboot recovery has not been tested.

## Verify

```bash
systemctl is-active sshuttle-vml.service
curl --fail http://10.255.0.1:3000/server_info
```

In the UI, launch `deployment_smoke_test`. Confirm SUCCESS and inspect the run's
`docker/container_id` and `dagster/image` tags. Docker should show a separate
pipeline container that exited with code 0, with no published ports or Docker
socket mount. Compute logs should remain visible in the UI.

Reference: [Dagster Docker example](https://github.com/dagster-io/dagster/tree/master/examples/deploy_docker).

Verified on 2026-09-19 after migration: all four Compose services healthy;
`deployment_smoke_test` launched through the browser and succeeded as run
`e6ae3662-b846-4526-824f-de1e9ffcf62b`. Independent container `a56afc6ffbf5`
used `vml-dagster-pipelines:12da959e61b839ac` and exited with code 0. Its shared
compute logs were readable in the UI after exit. The run container had no
published ports and no Docker socket mount. Public ports 3000, 4000, and 5432
were unreachable. Pre-migration configuration and PostgreSQL backup are in
`/opt/dagster/backups` on vml.
