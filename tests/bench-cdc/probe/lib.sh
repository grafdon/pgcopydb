# Names, the shared podman lock and container helpers of the probe. Source it.
# Every object it creates carries the prefix probe-; it removes nothing else.
# shellcheck shell=bash

P=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# no default: a default lock would let two users of the podman machine run at once
LOCK=${BENCH_LOCK:?probe: set BENCH_LOCK to the shared podman lock directory}
PG_IMAGE=${PG_IMAGE:-docker.io/library/postgres:18.7}
SRC_CPUS=${SRC_CPUS:-4}
# target quota plus client quota is the 14 vCPUs of the podman machine
CLIENT_CPUS=${CLIENT_CPUS:-6}
ME="${PROBE_OWNER:-probe} $$"
locked=0

# runbook "Podman lock": wait for it, never break it, release only our own
lock() {
  local warned=0
  until mkdir "$LOCK" 2>/dev/null; do
    if ((!warned)) && (($(date +%s) - $(stat -f %m "$LOCK" 2>/dev/null || date +%s) > 10800)); then
      echo "probe: $LOCK is older than 3 hours (owner: $(cat "$LOCK/owner" 2>/dev/null)); report it, still waiting" >&2
      warned=1
    fi
    sleep 20
  done
  echo "$ME" >"$LOCK/owner"
  locked=1
}
unlock() {
  ((locked)) || return 0
  [[ $(cat "$LOCK/owner" 2>/dev/null) == "$ME" ]] && rm -rf "$LOCK"
  locked=0
}

# a clean stop keeps the slots and the template database consistent on the volumes
down() {
  podman stop -t 60 probe-src probe-dst >/dev/null 2>&1 || true
  podman rm -f -t 5 probe-src probe-dst probe-client probe-run >/dev/null 2>&1 || true
}
teardown() {
  down
  podman volume rm -f probe-src-data probe-dst-data probe-work >/dev/null 2>&1 || true
  podman network rm -f probe-net >/dev/null 2>&1 || true
}

# the benchmark harness's server settings: no checkpoint and no autovacuum inside a measurement
pg_conf=(-c shared_buffers=1GB -c fsync=on -c synchronous_commit=on -c max_connections=300
  -c max_wal_size=16GB -c checkpoint_timeout=1h -c autovacuum=off)
pg_run() { # NAME CPUS VOLUME EXTRA...
  local name=$1 cpus=$2 vol=$3
  shift 3
  podman run -d --name "$name" --network probe-net --cpus "$cpus" --memory 6g --memory-swap 6g --shm-size 1g \
    -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=bench -v "$vol":/var/lib/postgresql \
    "$PG_IMAGE" "${pg_conf[@]}" "$@" >/dev/null
  local deadline=$((SECONDS + 120))
  # over TCP, because the entrypoint's init server listens on the socket only
  until podman exec "$name" pg_isready -q -h 127.0.0.1 -U postgres -d bench; do
    ((SECONDS < deadline)) || { echo "probe: $name not ready" >&2; return 1; }
    sleep 1
  done
}
up_src() { pg_run probe-src "$SRC_CPUS" probe-src-data -c wal_level=logical -c max_replication_slots=48 -c max_wal_senders=48; }
up_dst() { pg_run probe-dst "$1" probe-dst-data -c max_active_replication_origins=48; }
srcq() { podman exec probe-src psql -U postgres -d bench -XAtq -v ON_ERROR_STOP=1 -c "$1"; }
dstq() { podman exec probe-dst psql -U postgres -d "$1" -XAtq -v ON_ERROR_STOP=1 -c "$2"; }
cpu_usec() { podman exec "$1" awk '/^usage_usec/ { print $2 }' /sys/fs/cgroup/cpu.stat; }
# one-shot client container on the probe network, with the work volume
client() { podman run --rm --name probe-client --network probe-net --cpus "$CLIENT_CPUS" -v probe-work:/work "$@"; }
py() { # python in probe-py: scripts read-only at /probe, the results directory at /out
  podman run --rm --name probe-run --network probe-net --cpus "$CLIENT_CPUS" --timeout "${RUN_TIMEOUT:-3600}" \
    -e PYTHONHASHSEED=0 -e PROBE_WINDOW -v probe-work:/work -v "$P":/probe:ro -v "$OUT":/out localhost/probe-py python "$@"
}

# row count and order-independent checksum over all columns, one line per table
sums_sql="select format('%I.%I', n.nspname, c.relname), (xpath('/row/s/text()', query_to_xml(format(
  'select count(*) || '':'' || coalesce(sum(hashtextextended(t::text, 0)), 0) as s from %I.%I t',
  n.nspname, c.relname), false, true, '')))[1]::text
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where c.relkind = 'r' and n.nspname = 'public' order by 1"

image() { podman image exists localhost/probe-py || podman build -q -t probe-py -f "$P/Containerfile" "$P" >/dev/null; }
