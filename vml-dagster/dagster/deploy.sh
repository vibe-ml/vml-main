#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
set -a
source ../.env
set +a
: "${USERNAME:?}" "${HOSTNAME:?}" "${TARGET_DIR:?}" "${HOST_INT_IP:?}"
python3 - <<'PY'
import ipaddress, os
ip = ipaddress.ip_address(os.environ['HOST_INT_IP'])
assert ip.version == 4 and (ip == ipaddress.ip_address('0.0.0.0') or any(ip in ipaddress.ip_network(net) for net in (
    '10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'
))), 'HOST_INT_IP must be an RFC1918 private IPv4 address or 0.0.0.0'
assert os.environ['TARGET_DIR'].startswith('/') and os.environ['TARGET_DIR'] != '/', 'Use an absolute install directory other than /'
PY
port="${PORT:-}"
remote="$USERNAME@$HOSTNAME"
printf -v target '%q' "$TARGET_DIR"
printf -v bind_ip '%q' "$HOST_INT_IP"
is_local=0
if [ "$HOSTNAME" = "blanco" ] || [ "$HOSTNAME" = "localhost" ] || [ "$HOSTNAME" = "127.0.0.1" ] || [ "$HOSTNAME" = "$(hostname)" ]; then
  is_local=1
fi

archive_files=(compose.yaml Dockerfile Dockerfile.pipeline requirements.txt requirements-pipeline.txt dagster.yaml workspace.yaml .dockerignore code README.md)
if [ -f compose.override.yaml ]; then
  archive_files+=(compose.override.yaml)
fi
if [ "$is_local" -eq 1 ] && [ -f .env.blanco ]; then
  archive_files+=(.env.blanco)
fi

remote_script() {
  cat <<'REMOTE'
set -euo pipefail
cd "$1"
if [ "$2" != "0.0.0.0" ]; then
  ip -4 address show | grep -Fq "inet $2/" || { echo 'Private address is not configured'; exit 1; }
fi
umask 077
if [ ! -f .env ]; then
  if [ -f .env.blanco ]; then
    cp .env.blanco .env
  else
    password=$(python3 -c 'import secrets; print(secrets.token_hex(32))')
    printf 'DAGSTER_POSTGRES_PASSWORD=%s\n' "$password" > .env
  fi
fi
# Keep the existing database credential on repeated deployments.
if grep -q '^HOST_INT_IP=' .env; then
  sed -i "s/^HOST_INT_IP=.*/HOST_INT_IP=$2/" .env
else
  printf 'HOST_INT_IP=%s\n' "$2" >> .env
fi
target_port="${3:-}"
if [ -n "$target_port" ]; then
  if grep -q '^PORT=' .env; then
    sed -i "s/^PORT=.*/PORT=$target_port/" .env
  else
    printf 'PORT=%s\n' "$target_port" >> .env
  fi
fi
# Match socket permissions without running infrastructure containers as root.
docker_gid=$(stat -c '%g' /var/run/docker.sock)
# Content-address the pipeline release, so running jobs keep their original image.
pipeline_revision=$(tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner -cf - Dockerfile.pipeline requirements-pipeline.txt code | sha256sum | cut -c1-16)
for setting in "DOCKER_GID=$docker_gid" "PIPELINE_IMAGE=vml-dagster-pipelines:$pipeline_revision"; do
  key=${setting%%=*}
  if grep -q "^$key=" .env; then
    sed -i "s/^$key=.*/$setting/" .env
  else
    printf '%s\n' "$setting" >> .env
  fi
done
chmod 600 .env
docker compose config --quiet
docker compose up -d --build --wait --wait-timeout 240
docker compose ps
REMOTE
}

if [ "$is_local" -eq 1 ]; then
  sudo -n mkdir -p "$TARGET_DIR" && sudo -n chown "$(id -u):$(id -g)" "$TARGET_DIR"
  tar -czf - "${archive_files[@]}" | tar -xzf - -C "$TARGET_DIR"
  if [ -f .env.blanco ] && [ ! -f "$TARGET_DIR/.env" ]; then
    cp .env.blanco "$TARGET_DIR/.env"
  fi
  bash -s -- "$TARGET_DIR" "$HOST_INT_IP" "$port" < <(remote_script)
else
  ssh_options=(-o BatchMode=yes -o ConnectTimeout=10)
  ssh "${ssh_options[@]}" "$remote" "sudo -n mkdir -p $target && sudo -n chown \$(id -u):\$(id -g) $target"
  tar -czf - "${archive_files[@]}" | ssh "${ssh_options[@]}" "$remote" "tar -xzf - -C $target"
  ssh "${ssh_options[@]}" "$remote" "bash -s -- $target $bind_ip $port" < <(remote_script)
fi
