#!/usr/bin/env bash
# report.py in the probe-py image, under the shared podman lock.
# Usage: report.sh [RESULTS_DIR]   (default results-full)
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"
OUT=$(cd "${1:-$P/results-full}" && pwd)
trap unlock EXIT
lock
image
podman run --rm --name probe-run -v "$P":/probe:ro -v "$OUT":/out:ro localhost/probe-py python /probe/report.py /out
