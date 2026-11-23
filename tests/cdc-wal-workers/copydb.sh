#! /bin/bash

set -euo pipefail

# --wal-workers and --wal-high-traffic-tables: every refused combination exits
# with a usage error, one WAL worker keeps today's names and behaviour, and
# three workers get their slots, publications and origins, which stream
# cleanup drops by catalog or by name pattern.

SRC=${PGCOPYDB_SOURCE_PGURI}
TGT=${PGCOPYDB_TARGET_PGURI}
BAD_ARGS=1
SOURCE=6

sql() { psql -X -q -At -v ON_ERROR_STOP=1 -d "$@"; }
check() { echo "${1}: want [${2}] got [${3}]"; test "${2}" = "${3}"; }
repeat() { printf "%${2}s" '' | tr ' ' "$1"; }

pgcopydb ping

#
# usage
#
n=0

# expect code "message" log command...: exits with code and logs the message
expect() {
    local want=$1 msg=$2 log=$3 rc=0
    shift 3
    "$@" > "${log}" 2>&1 || rc=$?
    if [ "${rc}" -ne "${want}" ] || ! grep -qF -- "${msg}" "${log}"; then
        cat "${log}"
        echo "FAIL: want exit ${want} and \"${msg}\", got exit ${rc}: $*" >&2
        exit 1
    fi
    echo "OK exit ${rc}: $*"
}

# usage code "message" command...: expect, each case in a fresh --dir
usage() {
    local want=$1 msg=$2
    shift 2
    n=$((n + 1))
    expect "${want}" "${msg}" "/tmp/usage-${n}.log" "$@" --dir "/tmp/usage-${n}"
}

refuse() { usage "${BAD_ARGS}" "$@"; }

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

        # vacuity guard: the longest accepted names pass validation and
        # reach the assignment, which finds no table in the empty source
        usage "${SOURCE}" 'entry "a.b" matches 0 published tables' \
              pgcopydb ${cmd} --wal-workers 16 --slot-name "$(repeat s 51)" \
              --origin "$(repeat o 59)" --wal-high-traffic-tables a.b,c.d
        usage "${SOURCE}" 'entry "a.b" matches 0 published tables' \
              env PGCOPYDB_WAL_WORKERS=2 PGCOPYDB_WAL_HIGH_TRAFFIC_TABLES=a.b \
              pgcopydb ${cmd} --slot-name mig_w --origin mig_t1x
    }
done

refuse 'Option --wal-workers 2 requires --follow' pgcopydb clone --wal-workers 2
refuse 'Option --wal-workers 2 requires --follow' \
       env PGCOPYDB_WAL_WORKERS=2 pgcopydb clone

# nothing above may have changed the databases
check 'slots after usage cases' 0 "$(sql "${SRC}" -c 'select count(*) from pg_replication_slots')"
check 'publications after usage cases' 0 "$(sql "${SRC}" -c 'select count(*) from pg_publication')"

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

    expect 0 'Removing schema pgcopydb' "/tmp/run-${label}-cleanup.log" \
           pgcopydb stream cleanup --dir "${dir}"

    # the catalog of a one-worker run says there are no worker objects
    if grep -qE 'Dropping (replication slots|replication origins|publications) matching' \
            "/tmp/run-${label}-cleanup.log"; then
        cat "/tmp/run-${label}-cleanup.log"
        echo "FAIL ${label}: one-worker cleanup swept WAL worker names" >&2
        exit 1
    fi

    check "${label} objects left after cleanup" '0 0 0' \
          "$(sql "${SRC}" -c 'select count(*) from pg_replication_slots') $(sql "${SRC}" -c 'select count(*) from pg_publication') $(sql "${TGT}" -c 'select count(*) from pg_replication_origin')"

    echo "OK run ${label}"
}

run env env PGCOPYDB_WAL_WORKERS=1 pgcopydb clone --follow

# the option wins over the environment; -vv logs the SQL of the publication
run option env PGCOPYDB_WAL_WORKERS=16 pgcopydb clone --follow --wal-workers 1 -vv

# the single publication keeps today's SQL text, byte for byte
for stmt in "SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pgcopydb') ORDER BY schemaname, tablename;" \
            'CREATE PUBLICATION "pgcopydb" FOR TABLE "ww"."a", "ww"."b";'; do
    if ! grep -qF -- "${stmt}" /tmp/run-option.log; then
        echo "FAIL one-worker publication SQL not logged: ${stmt}" >&2
        exit 1
    fi
done
echo "OK one-worker publication SQL"

#
# setup: three WAL workers get slots, per-table publications and origins
#
S=pgcopydb

sql "${SRC}" <<'SQL'
drop schema ww cascade;
create schema ww;
create table ww.p(id integer, v text) partition by range (id);
create table ww.p1 partition of ww.p for values from (0) to (1000);
create table ww.p2 partition of ww.p for values from (1000) to (2000);
create table ww.ip(id integer primary key, v text);
create table ww.ic() inherits (ww.ip);
alter table ww.ic add primary key (id);
create table ww.k(v text);
insert into ww.p select i, 'p' from generate_series(700, 1299) i;
insert into ww.ip select i, 'ip' from generate_series(1, 200) i;
insert into ww.ic select i, 'ic' from generate_series(1001, 1700) i;
insert into ww.k select 'k' from generate_series(1, 10);
SQL

# the assignment weighs these counters, which reach the view asynchronously
want='ic:700 ip:200 k:10 p1:300 p2:300'
for _ in $(seq 60); do
    got=$(sql "${SRC}" -c "select string_agg(relname || ':' || n_tup_ins, ' ' order by relname collate \"C\") from pg_stat_user_tables where schemaname = 'ww' and n_tup_ins > 0")
    [ "${got}" = "${want}" ] && break
    sleep 0.5
done
check 'source write counters' "${want}" "${got}"

# ww.k is high traffic, then heaviest first: ic, the p family, ip
expected_assignment() {
    sql "${SRC}" -c "select '3|ww.k ' || string_agg(format('%s|ww|%s|%s|%s', c.oid, c.relname, coalesce(pg_partition_root(c.oid)::oid, c.oid), case c.relname when 'k' then 0 when 'ic' then 1 else 2 end), ' ' order by c.oid) from pg_class c where c.relnamespace = 'ww'::regnamespace and c.relkind in ('r', 'p')"
}

stored_assignment() {
    sqlite3 -readonly -init /dev/null -batch -noheader "$1/schema/source.db" \
            "select (select workers || '|' || high_traffic from wal_worker_setup) || ' ' || group_concat(r, ' ') from (select oid || '|' || nspname || '|' || relname || '|' || family || '|' || worker as r from wal_worker_table order by oid)"
}

# a partitioned root publishes its leaves, any other table only itself
expected_publications() {
    sql "${SRC}" -c "select string_agg(pubname || '=' || tabs, ' ' order by pubname collate \"C\") from (select '${S}_t' || c.oid as pubname, case c.relkind when 'p' then (select string_agg('ww.' || l.relname, ',' order by l.relname collate \"C\") from pg_partition_tree(c.oid) t join pg_class l on l.oid = t.relid where t.isleaf) else 'ww.' || c.relname end as tabs from pg_class c where c.relnamespace = 'ww'::regnamespace and c.relkind in ('r', 'p')) x"
}

published() {
    sql "${SRC}" -c "select string_agg(pubname || '=' || tabs, ' ' order by pubname collate \"C\") from (select p.pubname, coalesce(string_agg(t.schemaname || '.' || t.tablename, ',' order by t.tablename collate \"C\"), '') as tabs from pg_publication p left join pg_publication_tables t on t.pubname = p.pubname where p.pubname <> '${S}_t1x' group by p.pubname) x"
}

check_setup() {
    local dir=$1 lsn
    lsn=$(sql "${SRC}" -c "select confirmed_flush_lsn from pg_replication_slots where slot_name = '${S}'")
    test -n "${lsn}"

    check "${dir} slots" "${S}:pgoutput:${lsn},${S}_w1:pgoutput:${lsn},${S}_w2:pgoutput:${lsn}" \
          "$(sql "${SRC}" -c "select string_agg(slot_name || ':' || plugin || ':' || confirmed_flush_lsn, ',' order by slot_name collate \"C\") from pg_replication_slots where slot_name ~ '^${S}(_w[0-9]+)?$'")"
    check "${dir} publication count" 6 \
          "$(sql "${SRC}" -c "select count(*) from pg_publication where pubname ~ '^${S}_t[0-9]+$'")"
    check "${dir} publications" "$(expected_publications)" "$(published)"
    check "${dir} publication ${S}" 0 \
          "$(sql "${SRC}" -c "select count(*) from pg_publication where pubname = '${S}'")"
    check "${dir} publications of ww.ic" "$(sql "${SRC}" -c "select '${S}_t' || 'ww.ic'::regclass::oid")" \
          "$(sql "${SRC}" -c "select string_agg(pubname, ',') from pg_publication_tables where schemaname = 'ww' and tablename = 'ic'")"
    check "${dir} origins" "${S}@${lsn},${S}_w1@${lsn},${S}_w2@${lsn}" \
          "$(sql "${TGT}" -c "select string_agg(roname || '@' || pg_replication_origin_progress(roname, true), ',' order by roname collate \"C\") from pg_replication_origin where roname ~ '^${S}(_w[0-9]+)?$'")"
    check "${dir} assignment" "$(expected_assignment)" "$(stored_assignment "${dir}")"
}

setup() {
    expect "${BAD_ARGS}" 'WAL workers are not implemented yet' "$1.log" \
           pgcopydb clone --follow --dir "$1" --wal-workers 3 --wal-high-traffic-tables ww.k
    check_setup "$1"
}

# cleanup dir "message": stream cleanup logs the message and leaves the decoys
cleanup() {
    expect 0 "$2" "$1-cleanup.log" pgcopydb stream cleanup --dir "$1"

    check "$1 slots after cleanup" "${S}_wx,${S}x_w1" \
          "$(sql "${SRC}" -c "select string_agg(slot_name, ',' order by slot_name collate \"C\") from pg_replication_slots")"
    check "$1 publications after cleanup" "${S}_t1x" \
          "$(sql "${SRC}" -c "select string_agg(pubname, ',') from pg_publication")"
    check "$1 origins after cleanup" "${S}_w" \
          "$(sql "${TGT}" -c "select string_agg(roname, ',') from pg_replication_origin")"
}

setup /tmp/setup-1

# decoys: near misses of the worker names that no cleanup may drop
sql "${SRC}" -c "select pg_create_logical_replication_slot('${S}_wx', 'pgoutput')" \
             -c "select pg_create_logical_replication_slot('${S}x_w1', 'pgoutput')" \
             -c "create publication ${S}_t1x" > /dev/null
sql "${TGT}" -c "select pg_replication_origin_create('${S}_w')" > /dev/null

# the catalog names the worker objects
cleanup /tmp/setup-1 "Dropping replication slot \"${S}_w2\""
if grep -qF 'Dropping replication slots matching' /tmp/setup-1-cleanup.log; then
    echo "FAIL cleanup with a catalog swept by pattern" >&2
    exit 1
fi

# without the catalog, cleanup matches the names
setup /tmp/setup-2
rm -rf /tmp/setup-2
mkdir /tmp/setup-2
cleanup /tmp/setup-2 "Dropping replication slots matching \"${S}_w<digits>\""
if grep -qF "Dropping replication slot \"${S}_w1\"" /tmp/setup-2-cleanup.log; then
    echo "FAIL cleanup without a catalog used worker names" >&2
    exit 1
fi

# a resume keeps the worker count, the list and every worker slot
setup /tmp/setup-3
expect "${SOURCE}" 'set up with --wal-workers 3 --wal-high-traffic-tables "ww.k", this run uses --wal-workers 2' \
       /tmp/setup-3-workers.log \
       pgcopydb clone --follow --resume --dir /tmp/setup-3 --wal-workers 2 --wal-high-traffic-tables ww.k
expect "${SOURCE}" 'this run uses --wal-workers 3 --wal-high-traffic-tables ""' \
       /tmp/setup-3-list.log \
       pgcopydb clone --follow --resume --dir /tmp/setup-3 --wal-workers 3
expect "${SOURCE}" 'this run uses --wal-workers 1' \
       /tmp/setup-3-one.log \
       pgcopydb clone --follow --resume --dir /tmp/setup-3

# vacuity guard: the same options pass the checks
expect "${BAD_ARGS}" "Re-using replication slot \"${S}\"" /tmp/setup-3-same.log \
       pgcopydb clone --follow --resume --dir /tmp/setup-3 --wal-workers 3 --wal-high-traffic-tables ww.k
grep -qF 'WAL workers are not implemented yet' /tmp/setup-3-same.log

sql "${SRC}" -c "select pg_drop_replication_slot('${S}_w2')" > /dev/null
expect "${SOURCE}" "Replication slot \"${S}_w2\" does not exist" /tmp/setup-3-slot.log \
       pgcopydb clone --follow --resume --dir /tmp/setup-3 --wal-workers 3 --wal-high-traffic-tables ww.k
check 'slot never created on resume' 0 \
      "$(sql "${SRC}" -c "select count(*) from pg_replication_slots where slot_name = '${S}_w2'")"

cleanup /tmp/setup-3 "Dropping replication slot \"${S}_w1\""
