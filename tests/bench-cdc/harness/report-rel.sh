#!/usr/bin/env bash
# Medians [min..max] per workload, variant and phase over valid runs, ratios
# against REF_VARIANT (default V26-1), and every run left out and why, including
# the attempts the matrix superseded (*.prev-*), which never count toward the medians.
# Usage: report-rel.sh RESULTSDIR...   (environment: REF_VARIANT)
set -euo pipefail

(($#)) || { echo "usage: report-rel.sh RESULTSDIR..." >&2; exit 2; }
files=() noresult=''
for r in "$@"; do
  for d in "$r"/*/; do
    d=${d%/}
    if [[ -f $d/result.json ]]; then files+=("$d/result.json"); else noresult+="${d##*/}"$'\n'; fi
  done
done
((${#files[@]})) || { echo "report-rel.sh: no result.json under $*" >&2; exit 1; }

jq -rn --arg noresult "$noresult" --arg ref "${REF_VARIANT:-V26-1}" '
  def median: sort | length as $n | if $n == 0 then null elif $n % 2 == 1 then .[($n - 1) / 2] else (.[$n / 2 - 1] + .[$n / 2]) / 2 end;
  def f: if . == null then "-" elif (. | type) == "number" then (. * 1000 | round / 1000 | tostring) else tostring end;
  def stats($k): [.[] | .[$k] | select(type == "number")] | { med: median, min: min, max: max, n: length };
  def spread: if .med != null and .med > 0 and (.max - .min) / .med > 0.10 then "  SPREAD \(((.max - .min) / .med * 100) | round)%" else "" end;
  # time-like metrics: lower is better; rates: higher is better
  def rate: test("_(mbps|txn_s|rows_s)$");
  [inputs | . + { dir: (input_filename | split("/") | .[-2]), superseded: (input_filename | test("\\.prev-[0-9]+/result\\.json$")) }] as $all
  # the reference first, then every other variant the results hold
  | ($all | map(.variant) | unique) as $found
  | ([$ref | select(. as $r | $found | index($r))] + ($found - [$ref])) as $vs
  | ($all | map(select(.valid and (.superseded | not)))) as $ok
  | ($ok | group_by([.workload, .txns, .full_size])[] |
      .[0] as $g
      | stats("wal_mb") as $w
      | "== workload \($g.workload), txns \($g.txns), \(if $g.full_size == 1 then "full size" else "NOT full size (a setting differs from the defaults)" end), WAL \($w.med | f) MB [\($w.min | f)..\($w.max | f)]\($w | spread), valid runs \(length)",
        "   attempts: " + ([$vs[] as $v | ($all | map(select(.workload == $g.workload and .txns == $g.txns and .full_size == $g.full_size and .variant == $v)) | length) as $a
          | select($a > 0) | "\($v) \($a)"] | join(", ")),
        (["receive", "apply"][] as $p
          | "-- \($p) phase",
            (["s", "mbps", "txn_s", "rows_s", "cpu_runner_s", "cpu_src_s", "cpu_dst_s", "util_pod_pct", "util_src_pct", "util_dst_pct",
              "peak_anon_mb", "peak_mem_mb", "peak_spool_mb", "sat_pod_pct", "sat_src_pct", "sat_dst_pct", "sem_wait_pct", "host_cpu_pct"][] as $m
              | (if $m == "s" then $p + "_s" else $p + "_" + $m end) as $k
              | . as $runs
              | "   \($k)",
                ($vs[] as $v | ($runs | map(select(.variant == $v))) as $vr | ($vr | stats($k)) as $s
                  | select(($vr | length) > 0)
                  | "     \($v | . + "        " | .[0:8]) \($s.med | f) [\($s.min | f)..\($s.max | f)] n=\($s.n)"
                    + (if $s.n < ($vr | length) then " of \($vr | length) runs, \(($vr | length) - $s.n) without a value" else "" end)
                    + ($s | spread)),
                (($runs | map(select(.variant == $ref)) | stats($k)) as $b
                  | select($b.n > 0 and ($m == "s" or $m == "mbps" or $m == "cpu_runner_s"))
                  | ($vs[] | select(. != $ref)) as $v | ($runs | map(select(.variant == $v)) | stats($k)) as $s
                    | select($s.n > 0 and $b.med > 0)
                    | "     ratio \($v)/\($ref): medians \($s.med / $b.med | f), conservative "
                      + (if ($k | rate) then "\($s.min / $b.max | f) (worst \($v) / best \($ref))"
                         else "\($s.max / $b.min | f) (worst \($v) / best \($ref))" end)
                      + ", \($v) [\($s.min | f)..\($s.max | f)], \($ref) [\($b.min | f)..\($b.max | f)]"))),
        "-- extras (medians)",
        ($vs[] as $v | map(select(.variant == $v)) | select(length > 0)
          | "     \($v): receive_exit_s \(stats("receive_exit_s").med | f), spool_first_growth_s \(stats("spool_first_growth_s").med | f), apply_wake_s \(stats("apply_wake_s").med | f), apply_pse_s \(stats("apply_pse_s").med | f), exit_s \(stats("exit_s").med | f), rows_workload \(stats("rows_workload").med | f), rows_applied \(stats("rows_applied").med | f)")),
    "== runs left out or flagged",
    ($all[] | select((.valid | not) or .superseded)
      | "   \(.dir) \(.variant) \(.workload) txns=\(.txns)" + (if .superseded then " superseded" else "" end) + ": " +
        ([(.disqualified[]?), (if .contended then "contended: " + (.contended_by | join(", ")) else empty end),
          (if .receive_s == null or .apply_s == null then "phase time missing" else empty end),
          (if .valid then "valid, but moved aside by the matrix" else empty end)] | join("; "))),
    ($noresult | split("\n")[] | select(. != "") | "   \(.): no result"),
    ($ok[] | select((.receive_sat_pod_pct // 0) > 0 or (.apply_sat_pod_pct // 0) > 0 or (.receive_sat_src_pct // 0) > 0 or (.apply_sat_dst_pct // 0) > 0)
      | "   \(.label): saturated samples pod \(.receive_sat_pod_pct | f)%/\(.apply_sat_pod_pct | f)%, src \(.receive_sat_src_pct | f)%/\(.apply_sat_src_pct | f)%, dst \(.receive_sat_dst_pct | f)%/\(.apply_sat_dst_pct | f)% (receive/apply)"),
    ($ok[] | select([.receive_util_pod_pct, .receive_util_src_pct, .receive_util_dst_pct, .apply_util_pod_pct, .apply_util_src_pct, .apply_util_dst_pct] | map(. // 0) | max >= 90)
      | "   \(.label): mean CPU use of quota at 90% or more, pod \(.receive_util_pod_pct | f)%/\(.apply_util_pod_pct | f)%, src \(.receive_util_src_pct | f)%/\(.apply_util_src_pct | f)%, dst \(.receive_util_dst_pct | f)%/\(.apply_util_dst_pct | f)% (receive/apply)")
' "${files[@]}"
