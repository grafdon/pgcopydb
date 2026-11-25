#!/usr/bin/env bash
# pgcopydb steps inside the runner container, cut down from the benchmark
# harness's inner-rel.sh to one follow with --wal-workers 1.
# Usage: inner.sh init | endpos LSN | start | pid receive|apply | wait LINE SECS | stat PID
set -euo pipefail

W=/var/run/pgcopydb
D=$W/rel
L=$W/logs
FLOG=$L/follow.log
cmd=${1:?command}
mkdir -p "$L"
fargs=(--dir "$D" --plugin pgoutput --slot-name bench --origin bench --wal-workers 1)
src() { psql -XAtq -d "$PGCOPYDB_SOURCE_PGURI" -c "$1"; }

case $cmd in
  init)
    # follow creates the foreign keys at endpos from the clone's schema dump
    mkdir -p "$D/schema"
    cp "$W/base/schema/schema.dump" "$D/schema/"
    set -m
    pgcopydb follow "${fargs[@]}" >"$L/init.log" 2>&1 &
    pid=$!
    set +m
    deadline=$((SECONDS + 120))
    # a walsender running START_REPLICATION means the receive streams
    until [[ $(src "select count(*) from pg_replication_slots s join pg_stat_activity a on a.pid = s.active_pid
        where a.query like 'START_REPLICATION%' and s.slot_name = 'bench'") == 1 ]]; do
      ((SECONDS < deadline)) || { echo "inner.sh: the receive never streamed" >&2; exit 1; }
      kill -0 "$pid" 2>/dev/null || { echo "inner.sh: follow exited early" >&2; exit 1; }
      sleep 1
    done
    kill -TERM -- "-$pid"
    rc=0
    wait "$pid" || rc=$?
    if ((rc != 0)) && ! grep -q 'Foreign keys are pending' "$L/init.log"; then
      echo "inner.sh: follow exited $rc on TERM" >&2
      exit 1
    fi
    # prefetch: receive runs, apply waits for the sentinel
    pgcopydb stream sentinel set prefetch --dir "$D" 2>>"$L/sentinel.log"
    pgcopydb stream sentinel get --dir "$D" --json 2>>"$L/sentinel.log" | grep -Eq '"apply": *false' ||
      { echo "inner.sh: sentinel apply is not off" >&2; exit 1; }
    ;;
  endpos)
    pgcopydb stream sentinel set endpos --dir "$D" "${2:?LSN}" >/dev/null 2>>"$L/sentinel.log"
    ;;
  start)
    # run by 'podman exec -d': follow outlives the exec, its exit lands in follow.rc
    rm -f "$L/follow.rc"
    date +%s.%N >"$L/follow.start"
    rc=0
    pgcopydb follow --resume --not-consistent "${fargs[@]}" >"$FLOG" 2>&1 || rc=$?
    echo "$rc $(date +%s.%N)" >"$L/follow.rc"
    ;;
  pid)
    # the PID column of the first log line each subprocess writes
    case ${2:?receive or apply} in
      receive) mark='Streaming is setup to end at LSN' ;;
      apply) mark='Waiting until the pgcopydb sentinel apply is enabled' ;;
    esac
    for _ in $(seq 300); do
      pid=$(grep -m1 -F -- "$mark" "$FLOG" 2>/dev/null | awk '{ print $3 }' || true)
      [[ -n $pid ]] && { echo "$pid"; exit 0; }
      sleep 0.1
    done
    echo "inner.sh: no $2 process in the log" >&2
    exit 1
    ;;
  wait)
    # until LINE is in the follow log; prints its time as epoch seconds
    deadline=$((SECONDS + ${3:?SECS}))
    until grep -qF -- "${2:?LINE}" "$FLOG" 2>/dev/null; do
      if [[ -f $L/follow.rc ]]; then
        grep -qF -- "$2" "$FLOG" && break
        echo "inner.sh: follow exited without: $2 ($(cat "$L/follow.rc"))" >&2
        exit 1
      fi
      ((SECONDS < deadline)) || { echo "inner.sh: timeout waiting for: $2" >&2; exit 1; }
      sleep 0.2
    done
    grep -F -- "$2" "$FLOG" | tail -1 | { read -r d t _; TZ=UTC date -d "$d $t" +%s.%N; }
    ;;
  stat)
    # user and system ticks of PID twice a second until it exits: epoch utime stime
    while read -r -a f <"/proc/${2:?PID}/stat" 2>/dev/null; do
      echo "$(date +%s.%N) ${f[13]} ${f[14]}"
      sleep 0.5
    done
    ;;
  *)
    echo "inner.sh: unknown command $cmd" >&2
    exit 2
    ;;
esac
