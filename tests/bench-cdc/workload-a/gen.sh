#!/usr/bin/env bash
# Generate the pgbench scripts and, for N workers, one include-only filter
# file per worker using the plan's assignment rule 1.
# Usage: gen.sh OUTDIR N
set -euo pipefail

out=${1:?usage: gen.sh OUTDIR N}
n=${2:?usage: gen.sh OUTDIR N}
if ! [[ $n =~ ^[0-9]+$ ]] || ((n < 1 || n > 32)); then
  echo "gen.sh: N must be 1..32" >&2
  exit 2
fi

mkdir -p "$out/scripts"
rm -f "$out"/w*.ini

# Single-table scripts in Zipf rank order: name kind table.
singles='h1 upd h1
h2 upd h2
h3 upd h3
h4 upd h4
fk1 fk fk1
w01 upd w01
fk2 fk fk2
part upd part
w02 ins w02
kl1 upd kl1
fk3 fk fk3
w03 upd w03
w04 ins w04
kl2 upd kl2
fk4 fk fk4
w05 upd w05
w06 ins w06
w07 upd w07
w08 ins w08
w09 upd w09
w10 ins w10
w11 upd w11
w12 ins w12
w13 upd w13
w14 ins w14
w15 upd w15
w16 ins w16'

# Weights out of BIG_IN: big.sql 1, the multi-table scripts 10%, singles the
# rest by Zipf s=1.1. At 1 in 10000, big.sql writes 4x the other rows put
# together, so one stream applying it bounds every N >= 2.
big_in=${BIG_IN:-100000}
((big_in >= 1000 && big_in % 20 == 0)) || { echo "gen.sh: BIG_IN must be a multiple of 20, at least 1000" >&2; exit 2; }
weights=$(awk -v total=$((big_in * 9 / 10 - 1)) '
  { name[NR] = $1 }
  END {
    for (i = 1; i <= NR; i++) h += i ^ -1.1
    for (i = 1; i <= NR; i++) { w = int(total * i ^ -1.1 / h + 0.5); print name[i], (w < 1 ? 1 : w) }
  }' <<<"$singles")
weights+=$'\n'"multi_a $((big_in / 20))"$'\n'"multi_b $((big_in / 20))"$'\nbig 1'

upd() { printf '\\set id random(1, 100000)\nupdate %s set v = v + 1, ts = clock_timestamp() where id = :id;\n' "$1"; }

while read -r name kind table; do
  case $kind in
    upd) upd "$table" ;;
    ins) printf '\\set k random(1, 1000)\ninsert into %s (k, v, payload) values (:k, 0, repeat(md5(:k::text), 3));\n' "$table" ;;
    fk) printf '%s\n' '\set k random(1, 1000)' 'begin;' \
          "insert into ${table}_p (k, v, payload) values (:k, 0, repeat(md5(:k::text), 3)) returning id as pid \\gset" \
          "insert into ${table}_c (parent_id, v, payload) values (:pid, 0, 'c'), (:pid, 1, 'c'), (:pid, 2, 'c');" \
          'end;' ;;
  esac >"$out/scripts/$name.sql"
done <<<"$singles"

multi() {
  printf '\\set a random(1, 100000)\n\\set b random(1, 100000)\n\\set c random(1, 100000)\nbegin;\n'
  printf 'update %s set v = v + 1, ts = clock_timestamp() where id = :a;\n' "$1"
  printf 'update %s set v = v + 1, ts = clock_timestamp() where id = :b;\n' "$2"
  printf 'update %s set v = v + 1, ts = clock_timestamp() where id = :c;\nend;\n' "$3"
}
# Under rule 1 these triples sit on three workers each for N = 4 and 8; the
# self-check below fails if a weight change moves them.
ma='h1 h2 h4' mb='h1 h3 h4'
# shellcheck disable=SC2086 # word lists
{ multi $ma >"$out/scripts/multi_a.sql"; multi $mb >"$out/scripts/multi_b.sql"; }
big_rows=50000
printf '%s\n' "\\set s random(1, $((100001 - big_rows)))" \
  "update w01 set v = v + 1, ts = clock_timestamp() where id between :s and :s + $((big_rows - 1));" \
  >"$out/scripts/big.sql"

awk '{ printf "-f scripts/%s.sql@%d\n", $1, $2 }' <<<"$weights" >"$out/pgbench.args"

# Units in tie-break index order: name, weight, tables. The weight is the
# expected rows, as the plan's rule 1 weighs n_tup_ins + n_tup_upd + n_tup_del:
# script weight times the rows it writes there.
w() { awk -v s="$1" '$1 == s { print $2 }' <<<"$weights"; }
{
  for t in h1 h2 h3 h4; do
    x=$(w "$t")
    [[ " $ma " == *" $t "* ]] && x=$((x + $(w multi_a)))
    [[ " $mb " == *" $t "* ]] && x=$((x + $(w multi_b)))
    echo "$t $x public.$t"
  done
  for i in 1 2 3 4; do
    echo "fk${i}_p $(w "fk$i") public.fk${i}_p"
    echo "fk${i}_c $((3 * $(w "fk$i"))) public.fk${i}_c"
  done
  echo "kl1 $(w kl1) public.kl1"
  echo "kl2 $(w kl2) public.kl2"
  echo "part $(w part) public.part,public.part_0,public.part_1,public.part_2,public.part_3"
  echo "idle 0 public.idle"
  for i in $(seq -w 1 16); do
    x=$(w "w$i")
    [[ $i == 01 ]] && x=$((x + big_rows * $(w big)))
    echo "w$i $x public.w$i"
  done
} | awk '{ print NR - 1, $0 }' >"$out/units.tsv"

# Rule 1: heaviest unit first onto the least-loaded worker; ties by unit
# index, then by worker index.
sort -k3,3nr -k1,1n "$out/units.tsv" | awk -v n="$n" '
  BEGIN { for (k = 0; k < n; k++) load[k] = 0 }
  {
    best = 0
    for (k = 1; k < n; k++) if (load[k] < load[best]) best = k
    load[best] += $3
    print best, $2, $3, $4
  }' | sort -k1,1n >"$out/assign.tsv"

while read -r k _ _ tables; do
  f="$out/w$k.ini"
  [[ -f $f ]] || echo '[include-only-table]' >"$f"
  tr ',' '\n' <<<"$tables" >>"$f"
done <"$out/assign.tsv"

# Self-checks: every table exactly once, no idle worker, the family whole,
# and for N >= 4 each multi script spans three workers.
fail() { echo "gen.sh: self-check failed: $*" >&2; exit 1; }
[[ $(wc -l <"$out/units.tsv") -eq 32 ]] || fail "expected 32 units"
dups=$(cat "$out"/w*.ini | grep -v '^\[' | sort | uniq -d)
[[ -z $dups ]] || fail "table in two workers: $dups"
[[ $(cat "$out"/w*.ini | grep -vc '^\[') -eq 36 ]] || fail "expected 36 tables"
for ((k = 0; k < n; k++)); do [[ -f $out/w$k.ini ]] || fail "worker $k has no table"; done
[[ $(grep -l 'public.part' "$out"/w*.ini | wc -l) -eq 1 ]] || fail "partition family split"
worker_of() { awk -v t="$1" '$2 == t { print $1 }' "$out/assign.tsv"; }
if ((n >= 4)); then
  for m in "$ma" "$mb"; do
    # shellcheck disable=SC2086
    ws=$(for t in $m; do worker_of "$t"; done | sort -u | wc -l)
    ((ws == 3)) || fail "multi tables $m share a worker"
  done
fi

[[ $(awk '{ s += $2 } END { print s }' <<<"$weights") -eq $big_in ]] || fail "weights do not sum to $big_in"
# At the default BIG_IN, big.sql makes w01 a quarter of all rows, so rule 1
# gives it a worker of its own from N = 4 on, as the product would.
if ((n >= 4 && big_in == 100000)); then
  [[ $(awk -v k="$(worker_of w01)" '$1 == k' "$out/assign.tsv" | wc -l) -eq 1 ]] || fail "w01 shares a worker"
fi

echo "gen.sh: N=$n, $(wc -l <"$out/pgbench.args" | tr -d " ") scripts, weight total $(awk '{ s += $2 } END { print s }' <<<"$weights")"
awk '{ load[$1] += $3; tot += $3; u[$1] = u[$1] " " $2 }
  END { for (k in load) printf "  w%s rows %d (%.1f%%):%s\n", k, load[k], 100 * load[k] / tot, u[k] }' "$out/assign.tsv" | sort -V
