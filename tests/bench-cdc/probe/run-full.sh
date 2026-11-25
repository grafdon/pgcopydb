#!/usr/bin/env bash
# The full matrix: X1, X3 and X4 on workload A, then X3 and X4 on the hot workload H.
# Every step takes the shared podman lock itself, as owner "probe-full".
# A rerun skips what is done and valid, and continues an interrupted workload on
# its own backlog while the probe-* volumes of that setup still exist.
# The last line is "PROBE-FULL DONE" or "PROBE-FULL FAILED <reason>".
# Usage: run-full.sh   (environment: OUT, A_TXNS, H_TXNS, X4_REPS)
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=${OUT:-$here/results-full}
a_txns=${A_TXNS:-1280000}
h_txns=${H_TXNS:-300000}
# a replay takes seconds, so every X4 configuration runs three times
reps_x4=${X4_REPS:-3}
export PROBE_OWNER=probe-full
started=$SECONDS
failed=()

log() { echo "$(date +%T) $*"; }
die() { echo "PROBE-FULL FAILED $*"; exit 1; }
# a step runs in the background, so a signal reaches the trap while it runs;
# the step then stops its containers and releases the lock before we exit
child=''
step() {
  log "step: $(basename "$1") ${*:2}"
  "$here/probe.sh" "$@" &
  child=$!
  wait "$child"
}
# the step's shell sees the signal only when its container command returns, so end that one too
trap 'kill "$child" 2>/dev/null; podman rm -f -t 5 probe-run probe-client >/dev/null 2>&1; wait; die "interrupted"' INT TERM
valid() { [[ -f $1 ]] && jq -e .valid "$1" >/dev/null 2>&1; }
# the volumes of this directory's setup are still there
have_setup() {
  [[ -f $1/setup.env ]] && [[ $(podman volume inspect -f '{{index .Labels "probe.setup"}}' probe-work 2>/dev/null) == "$(sed -n 's/^token=//p' "$1/setup.env")" ]]
}
x4() { # DIR POLICY N DST_CPUS; one retry, because another container makes a run not valid
  local f rep try
  for ((rep = 1; rep <= reps_x4; rep++)); do
    f=$1/x4-$2-n$3-c$4-r$rep.json
    for try in 1 2; do
      valid "$f" && break
      REP=$rep step "$1" x4 "$2" "$3" "$4" || true
    done
    valid "$f" || failed+=("$(basename "$1")/$(basename "$f" .json): $(jq -c '{ok, equal, contended}' "$f" 2>/dev/null)")
  done
}

workload() { # NAME TXNS X1_REPETITIONS X3_WORKERS RUNS...
  local w=$1 txns=$2 reps=$3 sims=$4 o=$out/$1 rep sink run
  shift 4
  [[ -f $o/done ]] && { log "workload $w: done before"; return 0; }
  if ! have_setup "$o"; then
    # results of another backlog do not mix with the new one
    [[ -d $o ]] && mv "$o" "$o.prev-$(date +%s)"
    # two spare slots, for drains that a rerun has to repeat
    step "$o" setup "$w" "$txns" $((2 * reps + 2)) || die "$w: setup"
  fi
  for ((rep = 1; rep <= reps; rep++)); do
    for sink in null file; do
      [[ -f $o/x1-$sink-$rep.json ]] || step "$o" x1 "$sink" || failed+=("$w/x1-$sink-$rep")
    done
  done
  # workload H needs the capture only
  [[ -f $o/x1-file-1.json ]] || step "$o" x1 file || die "$w: capture"
  [[ -f $o/index.json ]] || step "$o" index || die "$w: index"
  # the N = 1 replay comes first, the simulation takes its costs from it
  x4 "$o" affinity 1 8
  [[ -f $o/x3.json ]] || step "$o" x3 "$sims" || failed+=("$w/x3")
  for run in "$@"; do
    # shellcheck disable=SC2086 # run is "POLICY N DST_CPUS"
    x4 "$o" $run
  done
  step "$o" teardown || die "$w: teardown"
  touch "$o/done"
}

mkdir -p "$out"
log "full matrix into $out: A $a_txns transactions, H $h_txns transactions"
workload A "$a_txns" 3 1,2,4,8,16 \
  "table 1 8" "rr 1 8" "table 4 8" "rr 4 8" "affinity 4 8" "table 8 8" "rr 8 8" "affinity 8 8" \
  "affinity 16 8" "affinity 1 2" "affinity 8 2"
workload H "$h_txns" 0 1,2,4,8,16 "rr 1 8" "rr 8 8" "affinity 8 8"
log "elapsed $(((SECONDS - started) / 60)) min"
((${#failed[@]} == 0)) || die "not valid or missing: ${failed[*]}"
echo "PROBE-FULL DONE"
