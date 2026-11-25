#!/usr/bin/env bash
# The small matrix that proves the tooling: every step on workload A and on the
# hot workload H, workload M for the kinds of change those two never make, the
# negative controls, and A with many 50,000-row transactions.
# Usage: smoke.sh [OUTDIR [PART...]]   parts: A H M A-big (default all, about 5 minutes)
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=${1:-$here/results-smoke}
shift || true
(($#)) || set -- A H M A-big
step() { echo "== $(basename "$1") ${*:2}"; "$here/probe.sh" "$@"; }

"$here/probe.sh" "$out" selftest
for part in "$@"; do
  o=$out/$part
  rm -rf "$o"
  case $part in
    A) step "$o" setup A 64000 2 ;;
    H) step "$o" setup H 30000 2 ;;
    M) step "$o" setup M 16000 2 ;;
    # 1 transaction in 1000 updates 50,000 rows: the large-transaction paths
    A-big) BIG_IN=1000 step "$o" setup A 16000 2 ;;
    *) echo "smoke.sh: unknown part $part" >&2; exit 2 ;;
  esac
  step "$o" x1 null
  step "$o" x1 file
  step "$o" index
  step "$o" x4 affinity 1
  step "$o" x3 1,4,8
  step "$o" x4 affinity 8
  step "$o" x4 rr 8
  case $part in
    H | M)
      # the gate must be able to fail: no dependency waits on a workload full of conflicts
      step "$o" x4 rr 8 8 --no-waits || echo "== negative control: the replay failed, as it may"
      jq -e '.valid | not' "$o/x4-rr-n8-c8-nowaits.json" >/dev/null ||
        { echo "smoke.sh: the negative control passed the gate" >&2; exit 1; }
      ;;&
    M | A-big)
      step "$o" x4 table 8
      step "$o" x4 affinity 16
      step "$o" x4 affinity 8 2
      ;;
  esac
  bad=$(jq -r 'select(.waits_on != false and (.valid | not)) | input_filename' "$o"/x4-*.json)
  [[ -z $bad ]] || { echo "smoke.sh: not valid: $bad" >&2; exit 1; }
done
"$here/probe.sh" "$out" teardown
echo "SMOKE DONE: $*"
