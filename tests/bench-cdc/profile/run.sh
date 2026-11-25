#!/usr/bin/env bash
# X2: one pgcopydb follow at one worker on workload A under perf, on fresh
# probe-* containers under the shared podman lock: clone, prefetch with apply
# off (receive profiled), then apply (profiled). Raw data stays on probe-work.
# Usage: run.sh OUT [TXNS]   (PERF_S: seconds of apply to sample, default 40)
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../probe/lib.sh
source "$here/../probe/lib.sh"
OUT=${1:?usage: run.sh OUT [TXNS]}
txns=${2:-320000}
perf_s=${PERF_S:-40}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
W=/var/run/pgcopydb
log() { echo "$(date +%T) $*" | tee -a "$OUT/run.log"; }
env_() { echo "$1=$2" >>"$OUT/run.env"; }
rx() { podman exec probe-runner "$@"; }
root() { podman exec -u root probe-runner "$@"; }
paranoid=''
finish() {
  local rc=$?
  if ((locked)); then
    podman cp probe-runner:$W/logs "$OUT/" >/dev/null 2>&1 || true
    podman rm -f -t 5 probe-runner >/dev/null 2>&1 || true
    down
    # give the VM its perf setting back
    [[ -z $paranoid ]] || podman machine ssh sudo sysctl -q -w kernel.perf_event_paranoid="$paranoid" || true
  fi
  unlock
  exit "$rc"
}
trap finish EXIT
trap 'exit 130' INT TERM
lock
: >"$OUT/run.env"
podman rm -f -t 5 probe-runner >/dev/null 2>&1 || true
teardown
free=$(($(podman machine ssh df -B1 --output=avail / | tail -1) >> 30))
((free >= 20)) || { log "only $free GiB free in the podman machine"; exit 1; }
# level 1 lets an unprivileged perf sample kernel time of its own processes
paranoid=$(podman machine ssh sysctl -n kernel.perf_event_paranoid | tr -d '\r')
podman machine ssh sudo sysctl -q -w kernel.perf_event_paranoid=1
env_ txns "$txns"
env_ others_start "$(podman ps --format '{{.Names}}' | grep -vc '^probe-' || true)"

podman network create probe-net >/dev/null
for v in probe-src-data probe-dst-data probe-work; do podman volume create "$v" >/dev/null; done
up_src
up_dst 2
# the benchmark's runner pod: 8 CPUs, 8 GiB; privileged for perf
podman run -d --name probe-runner --privileged --network probe-net --cpus 8 --memory 8g --memory-swap 8g \
  -v probe-work:$W -v "$here":/x2:ro \
  -e PGCOPYDB_SOURCE_PGURI=postgres://postgres@probe-src:5432/bench \
  -e PGCOPYDB_TARGET_PGURI=postgres://postgres@probe-dst:5432/bench \
  localhost/probe-pgcopydb-prof sleep infinity >/dev/null
root chown docker:postgres $W
rx pgcopydb --version 2>/dev/null | head -1 | tee "$OUT/version.txt"

log "schema and seed"
mkdir -p "$OUT/gen"
"$P/../workload-a/gen.sh" "$OUT/gen" 1 >"$OUT/gen.log"
podman exec -i probe-src psql -U postgres -d bench -Xq -f - <"$P/../workload-a/schema.sql" >"$OUT/schema.log" 2>&1
log "base clone"
rx pgcopydb clone --dir $W/base --table-jobs 4 --index-jobs 4 >"$OUT/clone.log" 2>&1
log "init: follow until the receive streams, then prefetch mode"
rx /x2/inner.sh init
srcq checkpoint
log "backlog: $txns transactions, no consumer attached"
# shellcheck disable=SC2046 # the pgbench arguments are a word list
podman run --rm --name probe-client --network probe-net --cpus 8 -v "$OUT/gen":/gen:ro -w /gen "$PG_IMAGE" \
  pgbench -n -c 16 -j 4 --random-seed=4242 -t $((txns / 16)) $(tr '\n' ' ' <"$OUT/gen/pgbench.args") \
  postgres://postgres@probe-src:5432/bench >"$OUT/pgbench.log" 2>&1
ran=$(awk '/^number of transactions actually processed/ { print $NF; exit }' "$OUT/pgbench.log" | cut -d/ -f1)
[[ $ran == "$txns" ]] || { log "pgbench ran $ran of $txns transactions"; exit 1; }
endpos=$(srcq "select pg_current_wal_flush_lsn()")
env_ endpos "$endpos"
env_ wal_bytes "$(srcq "select pg_wal_lsn_diff('$endpos', confirmed_flush_lsn)::bigint from pg_replication_slots where slot_name = 'bench'")"
rx /x2/inner.sh endpos "$endpos"
# WAL past endpos, so the receive meets a record or keepalive beyond it
srcq vacuum >/dev/null
srcq checkpoint

# cpu-clock with DWARF call graphs: the system libraries have no frame pointers
perf_rec="perf record -e cpu-clock -F 499 --call-graph dwarf,65528"
log "receive phase: follow --resume with apply off, endpos $endpos"
c0=$(cpu_usec probe-src)
podman exec -d probe-runner /x2/inner.sh start >/dev/null
rpid=$(rx /x2/inner.sh pid receive)
apid=$(rx /x2/inner.sh pid apply)
env_ receive_pid "$rpid"
env_ apply_pid "$apid"
podman exec -d probe-runner sh -c "/x2/inner.sh stat $rpid >$W/logs/receive.stat" >/dev/null
# perf ends with the receive process
podman exec -d -u root probe-runner sh -c "$perf_rec -p $rpid -o $W/receive.data >$W/logs/perf-receive.log 2>&1" >/dev/null
env_ follow_start "$(rx cat $W/logs/follow.start)"
env_ receive_end "$(rx /x2/inner.sh wait 'streamCloseFile: receive done at' 300)"
env_ source_cpu_receive_s "$(awk -v a="$c0" -v b="$(cpu_usec probe-src)" 'BEGIN { print (b - a) / 1e6 }')"
rx ps -eo pid,pcpu,etimes,args >"$OUT/ps-after-receive.txt"

log "apply phase: sentinel set apply"
d0=$(cpu_usec probe-dst)
r0=$(cpu_usec probe-runner)
podman exec -d probe-runner sh -c "/x2/inner.sh stat $apid >$W/logs/apply.stat" >/dev/null
env_ apply_on "$(rx date +%s.%N)"
rx pgcopydb stream sentinel set apply --dir $W/rel 2>>"$OUT/sentinel.log" >/dev/null
env_ apply_wake "$(rx /x2/inner.sh wait 'Applying up to' 60)"
log "perf on the apply process $apid for $perf_s s"
root sh -c "$perf_rec -p $apid -o $W/apply.data -- sleep $perf_s >$W/logs/perf-apply.log 2>&1" || log "perf record on apply failed"
rx ps -eo pid,pcpu,etimes,args >"$OUT/ps-during-apply.txt"
env_ apply_end "$(rx /x2/inner.sh wait 'pipeline_state_end: apply done at' 400)"
for _ in $(seq 100); do rx test -f $W/logs/follow.rc && break; sleep 0.2; done
env_ follow_rc "$(rx cat $W/logs/follow.rc | cut -d' ' -f1)"
env_ target_cpu_apply_s "$(awk -v a="$d0" -v b="$(cpu_usec probe-dst)" 'BEGIN { print (b - a) / 1e6 }')"
env_ runner_cpu_apply_s "$(awk -v a="$r0" -v b="$(cpu_usec probe-runner)" 'BEGIN { print (b - a) / 1e6 }')"
env_ clk_tck "$(rx getconf CLK_TCK)"
# kernel symbols are readable only while the perf setting is lowered
root sh -c "cat /proc/kallsyms >$W/kallsyms; chmod a+r $W/*.data $W/kallsyms; ls -l $W/*.data" | tee -a "$OUT/run.log"

# the run only counts when the target equals the source afterwards
podman exec probe-src psql -U postgres -d bench -XAtq -c "$sums_sql" >"$OUT/source.sums"
podman exec probe-dst psql -U postgres -d bench -XAtq -c "$sums_sql" >"$OUT/target.sums"
if diff "$OUT/source.sums" "$OUT/target.sums" >"$OUT/sums.diff"; then env_ equal true; else env_ equal false; fi
env_ others_end "$(podman ps --format '{{.Names}}' | grep -vc '^probe-' || true)"
awk -F= '{ v[$1] = $2 } END {
  printf "receive %.1f s; apply %.1f s, %.0f transactions per second; follow exit %s; equal %s\n",
    v["receive_end"] - v["follow_start"], v["apply_end"] - v["apply_on"],
    v["txns"] / (v["apply_end"] - v["apply_on"]), v["follow_rc"], v["equal"] }' "$OUT/run.env" | tee -a "$OUT/run.log"
