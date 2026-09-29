# VML Dagster

Containerized data pipelines on VML, deployed with Docker Compose. Dagster uses
separate infrastructure and pipeline images, with one container per run and up
to four concurrent runs.

## Deployed architecture

```mermaid
flowchart LR
    browser["Browser on local workstation"]
    tunnel["sshuttle over SSH to vml.pub"]
    browser --> tunnel

    subgraph vml["VML host"]
        socket["Host Docker socket"]
        subgraph network["Docker network: dagster_default"]
            web["Webserver :3000<br/>Private bind: 10.255.0.1"]
            daemon["Daemon<br/>Schedules, sensors, run queue"]
            code["Code server :4000<br/>Location: pipelines"]
            runs["Run containers<br/>Pipeline image, one per run"]
            db[("PostgreSQL :5432<br/>Run, event, schedule metadata")]
        end
        pgdata[("postgres_data volume")]
        storage[("dagster_dagster_storage volume<br/>Compute logs and local artifacts")]
        web -->|gRPC| code
        daemon -->|gRPC| code
        web --> db
        daemon --> db
        runs --> db
        db --- pgdata
        daemon -->|Launch runs| socket
        web -->|Manage runs| socket
        socket -->|Create| runs
        web --- storage
        daemon --- storage
        code --- storage
        runs --- storage
    end
    tunnel -->|Private HTTP| web
```

Only the webserver publishes a host port, bound to `HOST_INT_IP`. PostgreSQL,
the code server, and run containers publish no host ports. The UI has no login;
access relies on the private network and SSH tunnel.

## Deploy and use

Configure the root `.env` with `USERNAME`, `HOSTNAME`, `TARGET_DIR`, and
`HOST_INT_IP`. The remote host needs Docker Compose, Python 3, and the private
address configured. The SSH account needs Docker access and passwordless sudo
for directory setup. See the [tunnel setup](sshuttle/README.md).

From the repository root:

```bash
./dagster/deploy.sh
```

The script builds images remotely, preserves database credentials and data
volumes, and waits for healthy services. Open <http://10.255.0.1:3000> through
the tunnel and launch `deployment_smoke_test`.

- [Deployment directory layout](docs/deployment-layout.md)
- [Pipeline development and deployment](docs/dags-development.md)
- [Deployment configuration and operations](dagster/README.md)
- [SSH tunnel setup](sshuttle/README.md)
