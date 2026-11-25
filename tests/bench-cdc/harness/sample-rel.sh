#!/usr/bin/env bash
# bench-ww's sampler for the bench-rel-* containers and the host's other load,
# as CSV rows epoch,phase,container,metric,key,value. It leaves out the sentinel read,
# which takes the catalog semaphore the measured receive and apply use.
# Usage: sample-rel.sh RUNDIR | sample-rel.sh --once RUNDIR LABEL | sample-rel.sh --free | sample-rel.sh --machines
set -euo pipefail

free_bytes() {
  if podman machine inspect >/dev/null 2>&1; then
    podman machine ssh df -B1 --output=avail / | tail -1
  else
    df -B1 --output=avail "$(podman info -f '{{.Store.GraphRoot}}')" | tail -1
  fi
}
if [[ ${1:-} == --free ]]; then free_bytes; exit 0; fi
# shortcut: host load is read only on a podman machine; on Linux the bench
# containers are host processes, and others() alone sees contention
on_machine=$(podman machine inspect >/dev/null 2>&1 && echo 1 || echo 0)
# every other running podman machine: host_cpu() leaves its vCPUs out by
# process name, so it counts as contention; a failed listing counts too
machines() {
  ((on_machine)) || return 0
  podman machine list --format json |
    jq -r '.[] | select((.Running or .Starting) and .Name != "podman-machine-default") | .Name' ||
    echo podman-machine-list-failed
}
if [[ ${1:-} == --machines ]]; then machines; exit 0; fi
# summed %CPU of host processes other than the VM (vfkit runs the vCPUs in the
# Virtualization XPC process), a decayed average per process;
# the busiest five go to host.log as the evidence
host_cpu() {
  local ps
  ps=$(ps -A -o %cpu=,comm= | awk '$0 !~ /(vfkit|krunkit|gvproxy|qemu-system-[a-z0-9_]+|com\.apple\.Virtualization\.VirtualMachine)$/')
  echo "$(date +%s) $(sort -rn <<<"$ps" | head -5 | sed 's|^ *\([0-9.]*\) .*/|\1 |' | paste -sd'|' -)" >>"$dir/host.log"
  awk '{ s += $1 } END { printf "%.0f\n", s }' <<<"$ps"
}
once=false
if [[ ${1:-} == --once ]]; then once=true; shift; fi
dir=${1:?usage: sample-rel.sh [--once] RUNDIR [LABEL]}
min_free=$((${MIN_FREE_GIB:-6} << 30))
interval=${INTERVAL:-5}
quota="pod ${POD_CPUS:?} src ${SRC_CPUS:?} dst ${DST_CPUS:?}"
W=/var/run/pgcopydb

# the clock is read next to the counter in the container
# shellcheck disable=SC2016 # expanded by the container's shell
cgroup='echo cpu_t - $(date +%s.%N)
echo cpu_usec - $(awk "/^usage_usec/ { print \$2 }" /sys/fs/cgroup/cpu.stat)
echo mem_bytes - $(cat /sys/fs/cgroup/memory.current)
echo mem_peak - $(cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0)
echo anon_bytes - $(awk "/^anon / { print \$2 }" /sys/fs/cgroup/memory.stat)'
# shellcheck disable=SC2016
psql_q='psql -U postgres -d bench -XAtq -F " " -c "$Q"'

# spool per WAL worker directory, and how many pgcopydb processes sleep in a
# semaphore wait (10 reads 0.1 s apart)
# shellcheck disable=SC2016 # expanded by the runner's shell
runner='C='"$W"'/rel/cdc
set -- "$C"/w[0-9]*
if [ -d "$1" ]; then for d; do echo spool_bytes "${d##*/}" "$(du -sb "$d" | cut -f1)"; done
elif [ -d "$C" ]; then echo spool_bytes cdc "$(du -sb "$C" | cut -f1)"; fi
n=0 s=0
for i in 1 2 3 4 5 6 7 8 9 10; do
  for p in /proc/[0-9]*; do
    read -r c 2>/dev/null <"$p/comm" || continue
    [ "$c" = pgcopydb ] || continue
    read -r w 2>/dev/null <"$p/wchan" || w=gone
    n=$((n + 1)); case $w in *sem*) s=$((s + 1)) ;; esac
  done
  sleep 0.1
done
echo pg_procs - $((n / 10)); echo sem_wait_pct - $((n ? 100 * s / n : 0)); :'

src_sql="select 'wal_bytes', '-', pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint
union all select 'xact', '-', xact_commit from pg_stat_database where datname = current_database()
union all select 'rows', '-', coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del), 0)::bigint from pg_stat_user_tables
union all select 'slot_lag', slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint from pg_replication_slots
union all select 'spill_bytes', slot_name, spill_bytes from pg_stat_replication_slots
union all select 'spill_txns', slot_name, spill_txns from pg_stat_replication_slots"
dst_sql="select 'xact', '-', xact_commit from pg_stat_database where datname = current_database()
union all select 'rows', '-', coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del), 0)::bigint from pg_stat_user_tables"

prev=''
sample() {
  local phase=$1 t rows cur
  t=$(date +%s)
  # chained, so a failed exec drops the whole sample instead of its rows
  rows=$(
    podman exec -e Q="$src_sql" bench-rel-src sh -c "$cgroup; $psql_q" | sed 's/^/src /' &&
      podman exec -e Q="$dst_sql" bench-rel-dst sh -c "$cgroup; $psql_q" | sed 's/^/dst /' &&
      podman exec bench-rel-runner sh -c "$cgroup; $runner" | sed 's/^/run /' &&
      podman exec bench-rel-load sh -c "$cgroup" | sed 's/^/load /'
  ) || return 1
  ((on_machine)) && rows+=$'\n'"host host_cpu_pct - $(host_cpu)"
  machines | sed "s/^/$t /" >>"$dir/machines.log"
  cur=$(awk '{ g = ($1 == "run" || $1 == "load") ? "pod" : $1 }
    $2 == "cpu_usec" { c[g] += $4 } $2 == "cpu_t" && $1 != "load" { t[g] = $4 }
    END { for (g in c) printf "%s %d %.6f\n", g, c[g], t[g] }' <<<"$rows")
  # saturated: the group used at least 90% of its CPU quota since the last sample
  [[ -n $prev ]] && rows+=$'\n'$(awk -v q="$quota" '
    BEGIN { n = split(q, a, " "); for (i = 1; i < n; i += 2) lim[a[i]] = a[i + 1] }
    NR == FNR { pc[$1] = $2; pt[$1] = $3; next }
    ($1 in pt) && $3 > pt[$1] {
      pct = ($2 - pc[$1]) / ($3 - pt[$1]) / 1e4
      printf "%s cpu_pct - %.0f\n%s saturated - %d\n", $1, pct, $1, (pct >= 90 * lim[$1])
    }' <(echo "$prev") <(echo "$cur"))
  prev=$cur
  awk -v t="$t" -v p="$phase" 'NF == 4 { print t "," p "," $1 "," $2 "," $3 "," $4 }' <<<"$rows"
}

if $once; then
  sample "${2:?label}" >>"$dir/marks.csv"
  exit 0
fi

trap 'exit 0' TERM INT
next=$(date +%s)
while true; do
  phase=$(cat "$dir/phase" 2>/dev/null || echo unknown)
  sample "$phase" >>"$dir/samples.csv" || echo "sample-rel.sh: sample failed at $(date +%s)" >&2
  podman ps --format '{{.Names}}' | grep -v '^bench-rel-' | sed "s/^/$(date +%s) /" >>"$dir/others.log" || true
  # a failed check skips one reading; only a low reading stops the run
  if ! free=$(free_bytes); then
    echo "sample-rel.sh: disk check failed at $(date +%s)" >&2
    free=''
  fi
  [[ -n $free ]] && echo "$(date +%s),$phase,vm,free_bytes,-,$free" >>"$dir/samples.csv"
  if [[ -n $free ]] && ((free < min_free)); then
    echo "sample-rel.sh: $((free >> 20)) MiB free, below ${MIN_FREE_GIB:-6} GiB: removing the pod" >&2
    echo "disk_abort=$free" >>"$dir/run.env"
    podman pod rm -f -t 0 bench-rel-pod >/dev/null 2>&1 || true
    exit 1
  fi
  next=$((next + interval))
  now=$(date +%s)
  ((next > now)) && sleep $((next - now))
done
