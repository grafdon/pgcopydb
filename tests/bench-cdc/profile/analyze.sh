#!/usr/bin/env bash
# Turns the perf data of run.sh into text stacks in OUT, once, then classifies
# them. With the stacks in OUT the classifier runs again without the volume.
# Usage: analyze.sh OUT
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=../probe/lib.sh
source "$here/../probe/lib.sh"
OUT=$(cd "${1:?usage: analyze.sh OUT}" && pwd)
W=/var/run/pgcopydb
trap unlock EXIT
lock
vol=()
podman volume exists probe-work && vol=(-v "probe-work:$W:ro")
# shellcheck disable=SC2016 # expanded by the container's shell
podman run --rm --name probe-run -u root "${vol[@]}" -v "$here":/x2:ro -v "$OUT":/out \
  localhost/probe-pgcopydb-prof sh -c '
  set -e
  for p in apply receive; do
    # --no-inline: resolving inlined frames with addr2line takes minutes
    [ -s /out/$p.stacks ] || perf script -i '$W'/$p.data --kallsyms='$W'/kallsyms --no-inline \
      -F time,period,ip,sym,dso >/out/$p.stacks 2>/out/$p.script.err
    python3 /x2/classify.py $p /out/$p.stacks /out/$p.profile.json >/out/$p.profile.txt
  done'
