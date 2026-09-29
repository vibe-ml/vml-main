#!/usr/bin/env bash
# Retain daily pilot evidence and stop only OpenAlex schedules at the deadline.
set -euo pipefail
source /opt/dagster/pilot.env
umask 077
mkdir -p /opt/dagster/validation
finished=0
if [ "$(date -u +%s)" -ge "$PILOT_END_EPOCH" ]; then
  for schedule in daily_taxonomy_schedule daily_refresh_schedule process_batches_schedule; do
    docker exec dagster-daemon-1 dagster schedule stop \
      -w /opt/dagster/home/workspace.yaml -l openalex -r __repository__ "$schedule"
  done
  finished=1
fi
stamp=$(date -u +%Y%m%dT%H%M%SZ)
temporary=$(mktemp /opt/dagster/validation/measurement.XXXXXX)
trap 'rm -f "$temporary"' EXIT
"$PILOT_PYTHON" /opt/dagster/measure-openalex.py > "$temporary"
mv "$temporary" "/opt/dagster/validation/$stamp.json"
if [ "$finished" -eq 1 ]; then
  systemctl disable --now pilot-openalex.timer
fi
