#! /bin/bash

set -euo pipefail

# --wal-workers and --wal-high-traffic-tables: every refused combination exits
# with a usage error, and one WAL worker keeps today's names and behaviour.

SRC=${PGCOPYDB_SOURCE_PGURI}
TGT=${PGCOPYDB_TARGET_PGURI}
BAD_ARGS=1

sql() { psql -X -q -At -v ON_ERROR_STOP=1 -d "$@"; }
check() { echo "${1}: want [${2}] got [${3}]"; test "${2}" = "${3}"; }
repeat() { printf "%${2}s" '' | tr ' ' "$1"; }

pgcopydb ping

#
# usage
#
n=0

# refuse "message" command...: a usage error; each case gets a fresh --dir
refuse() {
    local msg=$1 rc=0
    shift
    n=$((n + 1))
    "$@" --dir "/tmp/usage-${n}" > "/tmp/usage-${n}.log" 2>&1 || rc=$?
    if [ "${rc}" -ne "${BAD_ARGS}" ] || ! grep -qF -- "${msg}" "/tmp/usage-${n}.log"; then
        cat "/tmp/usage-${n}.log"
        echo "FAIL case ${n}: want exit ${BAD_ARGS} and \"${msg}\", got exit ${rc}: $*" >&2
        exit 1
    fi
    echo "OK case ${n}: exit ${rc}: $*"
}

for cmd in "clone --follow" follow; do
    # shellcheck disable=SC2086
    {
        refuse 'Failed to parse --wal-workers "0"' pgcopydb ${cmd} --wal-workers 0
        refuse 'Failed to parse --wal-workers "17"' pgcopydb ${cmd} --wal-workers 17
        refuse 'Failed to parse --wal-workers "x"' pgcopydb ${cmd} --wal-workers x
        refuse '"PGCOPYDB_WAL_WORKERS": "17"' env PGCOPYDB_WAL_WORKERS=17 pgcopydb ${cmd}
        refuse '"PGCOPYDB_WAL_WORKERS": "0"' env PGCOPYDB_WAL_WORKERS=0 pgcopydb ${cmd}

        refuse 'requires --plugin pgoutput, found --plugin test_decoding' \
               pgcopydb ${cmd} --wal-workers 2 --plugin test_decoding
        refuse 'requires --plugin pgoutput, found --plugin wal2json' \
               env PGCOPYDB_WAL_WORKERS=2 PGCOPYDB_OUTPUT_PLUGIN=wal2json pgcopydb ${cmd}
        refuse 'not compatible with --publication "mine"' \
               pgcopydb ${cmd} --wal-workers 2 --publication mine
        refuse 'not compatible with --all-databases' \
               pgcopydb ${cmd} --wal-workers 2 --all-databases

        refuse 'slot name of at most 51 bytes' \
               pgcopydb ${cmd} --wal-workers 2 --slot-name "$(repeat s 52)"
        refuse 'origin of at most 59 bytes' \
               pgcopydb ${cmd} --wal-workers 2 --origin "$(repeat o 60)"
        refuse '--slot-name "mig_w1": names ending in _w<digits>' \
               pgcopydb ${cmd} --wal-workers 2 --slot-name mig_w1
        refuse '--slot-name "mig_t16384": names ending in _w<digits>' \
               pgcopydb ${cmd} --wal-workers 2 --slot-name mig_t16384
        refuse '--origin "mig_w12": names ending in _w<digits>' \
               pgcopydb ${cmd} --wal-workers 2 --origin mig_w12
        refuse '--origin "mig_t0": names ending in _w<digits>' \
               pgcopydb ${cmd} --wal-workers 2 --origin mig_t0

        refuse 'entry "public", expected schema.table' \
               pgcopydb ${cmd} --wal-workers 2 --wal-high-traffic-tables public
        refuse 'entry "a.b.c", expected schema.table' \
               pgcopydb ${cmd} --wal-workers 2 --wal-high-traffic-tables a.b.c
        refuse 'entry ".t", expected schema.table' \
               pgcopydb ${cmd} --wal-workers 2 --wal-high-traffic-tables .t
        refuse 'entry "s.", expected schema.table' \
               pgcopydb ${cmd} --wal-workers 2 --wal-high-traffic-tables s.
        refuse 'entry "s. t", expected schema.table' \
               pgcopydb ${cmd} --wal-workers 2 --wal-high-traffic-tables 's. t'
        refuse 'entry "", expected schema.table' \
               pgcopydb ${cmd} --wal-workers 3 --wal-high-traffic-tables a.b,,c.d
        refuse 'entry "", expected schema.table' \
               pgcopydb ${cmd} --wal-workers 2 --wal-high-traffic-tables a.b,
        refuse 'lists "a.b" more than once' \
               pgcopydb ${cmd} --wal-workers 3 --wal-high-traffic-tables a.b,a.b
        refuse 'lists more tables than --wal-workers 2' \
               pgcopydb ${cmd} --wal-workers 2 --wal-high-traffic-tables a.b,c.d,e.f
        refuse 'requires --wal-workers above 1, found --wal-workers 1' \
               pgcopydb ${cmd} --wal-high-traffic-tables a.b
        refuse 'requires --wal-workers above 1, found --wal-workers 1' \
               env PGCOPYDB_WAL_HIGH_TRAFFIC_TABLES=a.b pgcopydb ${cmd} --wal-workers 1
        refuse 'pgcopydb supports up to 2047 bytes' \
               pgcopydb ${cmd} --wal-workers 2 --wal-high-traffic-tables "$(repeat x 2048)"
        refuse 'pgcopydb only supports 2047 bytes' \
               env PGCOPYDB_WAL_HIGH_TRAFFIC_TABLES="$(repeat x 2048)" pgcopydb ${cmd}

        # vacuity guard: the longest accepted names pass validation
        refuse 'WAL workers are not implemented yet' \
               pgcopydb ${cmd} --wal-workers 16 --slot-name "$(repeat s 51)" \
               --origin "$(repeat o 59)" --wal-high-traffic-tables a.b,c.d
        refuse 'WAL workers are not implemented yet' \
               env PGCOPYDB_WAL_WORKERS=2 PGCOPYDB_WAL_HIGH_TRAFFIC_TABLES=a.b \
               pgcopydb ${cmd} --slot-name mig_w --origin mig_t1x
    }
done

refuse 'Option --wal-workers 2 requires --follow' pgcopydb clone --wal-workers 2
refuse 'Option --wal-workers 2 requires --follow' \
       env PGCOPYDB_WAL_WORKERS=2 pgcopydb clone

# nothing above may have reached the databases
check 'slots after usage cases' 0 "$(sql "${SRC}" -c 'select count(*) from pg_replication_slots')"

#
# one WAL worker: today's slot, origin and publication names
#
sql "${SRC}" <<'EOF'
create schema ww;
create table ww.a(id integer primary key, v text);
create table ww.b(id integer primary key, n bigint);
insert into ww.a select i, md5(i::text) from generate_series(1, 1000) i;
insert into ww.b select i, i * 2 from generate_series(1, 1000) i;
EOF

run() {
    local label=$1 dir=/tmp/run-$1 log=/tmp/run-$1.log pid rc=124
    shift

    # the previous run's post-data restore copied the publication as well
    sql "${TGT}" -c 'drop schema if exists ww cascade' \
                 -c 'drop publication if exists pgcopydb'

    "$@" --dir "${dir}" > "${log}" 2>&1 &
    pid=$!

    for _ in $(seq 240); do
        grep -q 'Applying CDC changes' "${log}" && break
        kill -0 "${pid}" 2> /dev/null || break
        sleep 0.5
    done
    if ! grep -q 'Applying CDC changes' "${log}"; then
        cat "${log}"
        echo "FAIL ${label}: apply did not start" >&2
        exit 1
    fi

    sql "${SRC}" -c "insert into ww.a select m + i, '${label}' from (select max(id) m from ww.a) x, generate_series(1, 100) i" \
                 -c "update ww.b set n = n + 1 where id % 3 = 0" \
                 -c "delete from ww.a where id % 10 = 0"

    pgcopydb stream sentinel set endpos --current --dir "${dir}"

    for _ in $(seq 240); do
        if ! kill -0 "${pid}" 2> /dev/null; then
            rc=0
            wait "${pid}" || rc=$?
            break
        fi
        sleep 0.5
    done
    if [ "${rc}" -ne 0 ]; then
        kill "${pid}" 2> /dev/null || true
        cat "${log}"
        echo "FAIL ${label}: clone --follow exit ${rc}" >&2
        exit 1
    fi

    pgcopydb compare data --dir "/tmp/compare-${label}"

    check "${label} slots" 'pgcopydb:pgoutput' \
          "$(sql "${SRC}" -c "select string_agg(slot_name || ':' || plugin, ',') from pg_replication_slots")"
    check "${label} publications" pgcopydb \
          "$(sql "${SRC}" -c "select string_agg(pubname, ',') from pg_publication")"
    check "${label} origins" pgcopydb \
          "$(sql "${TGT}" -c "select string_agg(roname, ',') from pg_replication_origin")"

    pgcopydb stream cleanup --dir "${dir}"

    check "${label} objects left after cleanup" '0 0 0' \
          "$(sql "${SRC}" -c 'select count(*) from pg_replication_slots') $(sql "${SRC}" -c 'select count(*) from pg_publication') $(sql "${TGT}" -c 'select count(*) from pg_replication_origin')"

    echo "OK run ${label}"
}

run env env PGCOPYDB_WAL_WORKERS=1 pgcopydb clone --follow

# the option wins over the environment
run option env PGCOPYDB_WAL_WORKERS=16 pgcopydb clone --follow --wal-workers 1
