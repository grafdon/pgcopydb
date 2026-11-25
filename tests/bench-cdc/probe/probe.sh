#!/usr/bin/env bash
# One probe step per call, under the shared podman lock, on fresh containers over
# the probe-* volumes; the containers are gone before the lock is released.
# Usage: probe.sh OUT setup A|H|M TXNS [SLOTS] | x1 null|file | index | x3 N[,N...]
#        probe.sh OUT x4 POLICY N [DST_CPUS] [--no-waits] | teardown | selftest
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

OUT=${1:?usage: probe.sh OUT STEP ...}
step=${2:?step}
shift 2
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
die() { echo "probe.sh $step: $*" >&2; exit 1; }
env_get() { sed -n "s/^$1=//p" "$OUT/setup.env"; }
# a measurement next to other containers is not valid
others() { podman ps --format '{{.Names}}' | grep -vc '^probe-' || true; }

finish() {
  local rc=$?
  ((locked)) && down
  unlock
  exit "$rc"
}
trap finish EXIT
trap 'exit 130' INT TERM
lock
down
image

case $step in
  setup)
    w=${1:?workload} txns=${2:?transactions} slots=${3:-2}
    ((txns % 16 == 0)) || die "TXNS must be a multiple of the 16 pgbench clients"
    rm -rf "$OUT/gen" "$OUT/setup.env"
    mkdir -p "$OUT/gen"
    case $w in
      A) "$P/../workload-a/gen.sh" "$OUT/gen" 1 >"$OUT/gen.log" || die "gen.sh failed, see $OUT/gen.log"
         schema=$P/../workload-a/schema.sql bench="--random-seed=4242 $(tr '\n' ' ' <"$OUT/gen/pgbench.args")" ;;
      H) bench="--random-seed=4244" ;;
      M) cp "$P/workload-m/mixed.sql" "$OUT/gen/"
         schema=$P/workload-m/schema.sql bench="--random-seed=4245 -f mixed.sql" ;;
      *) die "workload must be A, H or M" ;;
    esac
    free=$(($(podman machine ssh df -B1 --output=avail / | tail -1) >> 30))
    ((free >= 20)) || die "only $free GiB free in the podman machine, need 20"
    teardown
    token=$(date +%s)-$$
    podman network create probe-net >/dev/null
    podman volume create probe-src-data >/dev/null
    podman volume create probe-dst-data >/dev/null
    podman volume create --label probe.setup="$token" probe-work >/dev/null
    up_src
    up_dst 8
    if [[ $w == H ]]; then
      # TPC-B at scale 10: every transaction updates one of ten branch rows
      podman exec probe-src pgbench -i -q -s 10 -U postgres bench >"$OUT/schema.log" 2>&1
    else
      podman exec -i probe-src psql -U postgres -d bench -Xq -f - <"$schema" >"$OUT/schema.log" 2>&1
    fi
    echo "$(date +%T) base copy to the target template"
    dstq postgres "create database probe_base"
    podman exec probe-dst bash -o pipefail -c \
      'pg_dump -h probe-src -U postgres --no-publications bench | psql -U postgres -d probe_base -Xq -v ON_ERROR_STOP=1' \
      >"$OUT/base.log" 2>&1
    dstq probe_base "vacuum analyze"
    # one slot per drain, created while the source is quiet
    srcq "create publication probe for all tables"
    srcq "select pg_create_logical_replication_slot('probe_' || i, 'pgoutput') from generate_series(1, $slots) i" >/dev/null
    srcq checkpoint
    echo "$(date +%T) backlog: $txns transactions, no consumer attached"
    # shellcheck disable=SC2086 # bench is a word list
    podman run --rm --name probe-client --network probe-net --cpus 8 -v "$OUT/gen":/gen:ro -w /gen "$PG_IMAGE" \
      pgbench -n -c 16 -j 4 -t $((txns / 16)) $bench postgres://postgres@probe-src:5432/bench >"$OUT/pgbench.log" 2>&1
    ran=$(awk '/^number of transactions actually processed/ { print $NF; exit }' "$OUT/pgbench.log" | cut -d/ -f1)
    [[ $ran == "$txns" ]] || die "pgbench ran $ran of $txns transactions"
    endpos=$(srcq "select pg_current_wal_flush_lsn()")
    # WAL past endpos, so a drain meets a record or a keepalive beyond it
    srcq checkpoint
    podman exec probe-src psql -U postgres -d bench -XAtq -c "$sums_sql" >"$OUT/source.sums"
    echo 1 >"$OUT/slot.next"
    printf '%s\n' "workload=$w" "txns=$txns" "slots=$slots" "endpos=$endpos" "token=$token" >"$OUT/setup.env"
    echo "$(date +%T) setup done: workload $w, $txns transactions, endpos $endpos, $(wc -l <"$OUT/source.sums" | tr -d ' ') tables"
    ;;

  x1)
    sink=${1:?null or file}
    case $sink in null) file=/dev/null ;; file) file=/work/x1.bin ;; *) die "sink must be null or file" ;; esac
    slot=$(cat "$OUT/slot.next")
    ((slot <= $(env_get slots))) || die "no unused slot left"
    # a drain consumes its slot, also when it fails
    echo $((slot + 1)) >"$OUT/slot.next"
    endpos=$(env_get endpos)
    up_src
    wal=$(srcq "select pg_wal_lsn_diff('$endpos', confirmed_flush_lsn)::bigint from pg_replication_slots where slot_name = 'probe_$slot'")
    n0=$(others)
    c0=$(cpu_usec probe-src)
    # shellcheck disable=SC2016 # expanded by the container's shell
    line=$(client "$PG_IMAGE" bash -c '
      rm -f /work/x1.bin
      t0=$(date +%s.%N)
      pg_recvlogical -h probe-src -U postgres -d bench -S "$1" --start --no-loop \
        -o proto_version=1 -o publication_names=probe --endpos "$2" -f "$3" || exit 1
      t1=$(date +%s.%N)
      echo "$t0 $t1 $(stat -c %s "$3") $(awk "/^usage_usec/ { print \$2 }" /sys/fs/cgroup/cpu.stat)"
      # the first file drain is the capture every later step reads
      if [ "$3" != /dev/null ]; then [ -e /work/capture.bin ] && rm -f "$3" || mv "$3" /work/capture.bin; fi
      ' _ "probe_$slot" "$endpos" "$file")
    c1=$(cpu_usec probe-src)
    read -r t0 t1 bytes client_usec <<<"$line"
    rep=$(($(find "$OUT" -name "x1-$sink-*.json" | wc -l) + 1))
    jq -n --arg sink "$sink" --argjson slot "$slot" --argjson wal "$wal" --argjson t0 "$t0" --argjson t1 "$t1" \
      --argjson bytes "$bytes" --argjson c0 "$c0" --argjson c1 "$c1" --argjson client "$client_usec" \
      --argjson others $((n0 + $(others))) --argjson txns "$(env_get txns)" --arg workload "$(env_get workload)" \
      '{workload: $workload, txns: $txns, sink: $sink, slot: $slot, seconds: ($t1 - $t0), wal_bytes: $wal,
        capture_bytes: (if $sink == "file" then $bytes else null end), wal_mb_per_s: ($wal / 1e6 / ($t1 - $t0)),
        source_cpu_s: (($c1 - $c0) / 1e6), client_cpu_s: ($client / 1e6), contended: ($others > 0)}' \
      >"$OUT/x1-$sink-$rep.json"
    jq -c . "$OUT/x1-$sink-$rep.json"
    ;;

  index)
    py /probe/dispatch.py index /work/capture.bin /work/index.pkl /out/index.json
    # pg_recvlogical must have delivered every pgbench transaction
    jq -e --argjson t "$(env_get txns)" '.txns == $t' "$OUT/index.json" >/dev/null ||
      die "the capture holds $(jq .txns "$OUT/index.json") transactions, pgbench ran $(env_get txns)"
    ;;

  x4)
    policy=${1:?policy} n=${2:?workers} cpus=${3:-8} extra=${4:-}
    [[ $n$cpus =~ ^[0-9]+$ && ${extra:---no-waits} == --no-waits ]] || die "usage: x4 POLICY N [DST_CPUS] [--no-waits]"
    name=x4-$policy-n$n-c$cpus${extra:+-nowaits}${REP:+-r$REP}
    rm -f "$OUT/$name".{json,part.json,sums,diff}
    up_dst "$cpus"
    # every replay starts from a fresh copy of the base state
    dstq postgres "drop database if exists run"
    dstq postgres "create database run template probe_base strategy file_copy"
    n0=$(others)
    c0=$(cpu_usec probe-dst)
    rc=0
    # shellcheck disable=SC2086 # extra is one optional flag
    py /probe/replay.py /work/capture.bin /work/index.pkl "host=probe-dst user=postgres dbname=run" \
      "$policy" "$n" "/out/$name.part.json" $extra 2>&1 | tee "$OUT/$name.log" || rc=$?
    c1=$(cpu_usec probe-dst)
    [[ -s $OUT/$name.part.json ]] || echo '{"ok": false}' >"$OUT/$name.part.json"
    # the gate: per table the row count and checksum must equal the source's
    podman exec probe-dst psql -U postgres -d run -XAtq -c "$sums_sql" >"$OUT/$name.sums"
    equal=true
    diff "$OUT/source.sums" "$OUT/$name.sums" >"$OUT/$name.diff" || equal=false
    jq --arg workload "$(env_get workload)" --argjson cpus "$cpus" --argjson client "$CLIENT_CPUS" \
      --argjson c0 "$c0" --argjson c1 "$c1" --argjson equal "$equal" --argjson others $((n0 + $(others))) \
      --argjson differ "$(grep -c '^>' "$OUT/$name.diff" || true)" \
      '{workload: $workload, dst_cpus: $cpus, client_cpus: $client} + . +
       {dst_cpu_s: (($c1 - $c0) / 1e6), equal: $equal, tables_differ: $differ, contended: ($others > 0),
        valid: (.ok and $equal and $others == 0)}' "$OUT/$name.part.json" >"$OUT/$name.json"
    rm -f "$OUT/$name.part.json"
    dstq postgres "drop database run"
    echo "$name: replay exit $rc, $(jq -c '{ok, equal, tables_differ, valid, wall_s, tps, dst_cpu_s, client_cpu_s}' "$OUT/$name.json")"
    ;;

  x3)
    # the costs come from an N = 1 replay at the 8 CPU target, not from a throttled one
    fit=$(find "$OUT" -name 'x4-*-n1-c8*.json' | sort | head -1)
    [[ -n $fit ]] && jq -e '.valid and (.groups | length > 0)' "$fit" >/dev/null || die "no valid N = 1 replay to fit the cost from"
    py /probe/dispatch.py sim /work/index.pkl "/out/$(basename "$fit")" /out/x3.json "${1:?worker counts}" | tee "$OUT/x3.txt"
    ;;

  teardown)
    teardown
    ;;

  selftest)
    podman run --rm --name probe-run -v "$P":/probe:ro localhost/probe-py python /probe/selftest.py
    ;;

  *) die "unknown step" ;;
esac
