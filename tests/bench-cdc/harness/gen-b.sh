#!/usr/bin/env bash
# The pgbench script of workload B: 10 inserts and 2 updates per transaction,
# each on a table drawn Zipf s = 1.1 over b1..b8, so b1 and b2 are hottest.
# pgbench substitutes the table number into the name (b:t1 becomes b3).
# Usage: gen-b.sh OUTDIR
set -euo pipefail

out=${1:?usage: gen-b.sh OUTDIR}
mkdir -p "$out/scripts"
{
  for i in $(seq 1 10); do
    printf '\\set t%d random_zipfian(1, 8, 1.1)\n\\set r%d random(1, 2000000000)\n' "$i" "$i"
  done
  for i in 1 2; do
    printf '\\set u%d random_zipfian(1, 8, 1.1)\n\\set i%d random(1, 20000)\n' "$i" "$i"
  done
  echo 'begin;'
  for i in $(seq 1 10); do
    printf 'insert into b:t%d (k, v, payload) values (:r%d %% 1000, 0, rnd_payload(:r%d));\n' "$i" "$i" "$i"
  done
  for i in 1 2; do
    printf 'update b:u%d set v = v + 1, ts = clock_timestamp() where id = :i%d;\n' "$i" "$i"
  done
  echo 'end;'
} >"$out/scripts/b.sql"
echo '-f scripts/b.sql' >"$out/pgbench.args"
# self-check: 10 inserts and 2 updates in one transaction
[[ $(grep -c '^insert' "$out/scripts/b.sql") -eq 10 && $(grep -c '^update' "$out/scripts/b.sql") -eq 2 ]] ||
  { echo "gen-b.sh: self-check failed" >&2; exit 1; }
