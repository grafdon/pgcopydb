#! /bin/bash

set -euo pipefail

# --wal-workers and --wal-high-traffic-tables: every refused combination exits
# with a usage error, one WAL worker keeps today's names and behaviour, three
# workers get their slots, publications and origins, which stream cleanup
# drops by catalog or by name pattern, and three workers converge under load.

SRC=${PGCOPYDB_SOURCE_PGURI}
TGT=${PGCOPYDB_TARGET_PGURI}
BAD_ARGS=1
SOURCE=6
INTERNAL=12

sql() { psql -X -q -At -v ON_ERROR_STOP=1 -d "$@"; }
check() { echo "${1}: want [${2}] got [${3}]"; test "${2}" = "${3}"; }
repeat() { printf "%${2}s" '' | tr ' ' "$1"; }

# pgrep exits 1 when nothing matches, and 2 or more when it failed to look
no_pgcopydb() {
    local rc=0
    pgrep -a -x pgcopydb || rc=$?
    if [ "${rc}" -ne 1 ]; then
        echo "FAIL: pgcopydb processes are left after $1, pgrep exit ${rc}" >&2
        exit 1
    fi
}

# the walsender of a stopped worker lets go of its slot a little later
wait_slots_inactive() {
    for _ in $(seq 60); do
        [ "$(sql "${SRC}" -c 'select count(*) from pg_replication_slots where active')" -eq 0 ] && return
        sleep 0.5
    done
    echo "FAIL: replication slots still active" >&2
    exit 1
}

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

    if [ -e "${dir}/cdc/w0" ]; then
        echo "FAIL ${label}: one worker created ${dir}/cdc/w0" >&2
        exit 1
    fi

    check "${label} slots" 'pgcopydb:pgoutput' \
          "$(sql "${SRC}" -c "select string_agg(slot_name || ':' || plugin, ',') from pg_replication_slots")"
    check "${label} publications" pgcopydb \
          "$(sql "${SRC}" -c "select string_agg(pubname, ',') from pg_publication")"
    check "${label} origins" pgcopydb \
          "$(sql "${TGT}" -c "select string_agg(roname, ',') from pg_replication_origin")"

    # only clone and follow validate the worker options
    expect 0 'Removing schema pgcopydb' "/tmp/run-${label}-cleanup.log" \
           env PGCOPYDB_WAL_WORKERS=2 PGCOPYDB_WAL_HIGH_TRAFFIC_TABLES=a.b,c.d,e.f \
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

# snapshot --follow sets up one worker, whatever its environment says
env PGCOPYDB_WAL_WORKERS=2 PGCOPYDB_WAL_HIGH_TRAFFIC_TABLES=ww.a \
    pgcopydb snapshot --follow --plugin pgoutput --dir /tmp/snapshot \
    > /tmp/snapshot.out 2> /tmp/snapshot.log &
snapshot_pid=$!
for _ in $(seq 120); do
    test -s /tmp/snapshot.out && break
    kill -0 "${snapshot_pid}" 2> /dev/null || break
    sleep 0.5
done
if ! test -s /tmp/snapshot.out; then
    cat /tmp/snapshot.log
    echo "FAIL: snapshot --follow printed no snapshot" >&2
    exit 1
fi
check 'snapshot --follow slots' 'pgcopydb:pgoutput' \
      "$(sql "${SRC}" -c "select string_agg(slot_name || ':' || plugin, ',') from pg_replication_slots")"
check 'snapshot --follow publications' pgcopydb \
      "$(sql "${SRC}" -c "select string_agg(pubname, ',') from pg_publication")"
kill -TERM "${snapshot_pid}"
wait "${snapshot_pid}"
wait_slots_inactive
sql "${SRC}" -c "select pg_drop_replication_slot('pgcopydb')" \
             -c 'drop publication pgcopydb' > /dev/null

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

# the three worker catalogs name their own slot and moved output.db
check_worker_catalogs() {
    local dir=$1 k slot
    for k in 0 1 2; do
        slot=${S}; [ "${k}" -eq 0 ] || slot=${S}_w${k}
        check "${dir} catalog of worker ${k}" "${slot} 1" \
              "$(sqlite3 -readonly -init /dev/null -batch -noheader "${dir}/cdc/w${k}/source.db" \
                 "select (select slot_name from replication_slot) || ' ' || (select count(*) from cdc_files where filename like '${dir}/cdc/w${k}/%-output.db')")"
        test -s "$(sqlite3 -readonly -init /dev/null -batch -noheader "${dir}/cdc/w${k}/source.db" 'select filename from cdc_files order by id limit 1')"
    done
}

# the LSN at which the main catalog says the slots were created
slot_lsn() {
    sqlite3 -readonly -init /dev/null -batch -noheader "$1/schema/source.db" \
            "select printf('%X/%X', lsn >> 32, lsn & 4294967295) from replication_slot"
}

# receive may move a slot once it runs, so the copies are checked in the log
# of the run that created them: dir.log
check_setup() {
    local dir=$1 lsn
    lsn=$(slot_lsn "${dir}")
    test -n "${lsn}"

    check "${dir} slot copies" "${S}_w1@${lsn} ${S}_w2@${lsn}" \
          "$(sed -n "s/.*Copied replication slot \"${S}\" to \"\(${S}_w[0-9]*\)\" at \(.*\)$/\1@\2/p" "${dir}.log" | paste -sd ' ')"
    check "${dir} slots" "${S}:pgoutput:t,${S}_w1:pgoutput:t,${S}_w2:pgoutput:t" \
          "$(sql "${SRC}" -c "select string_agg(slot_name || ':' || plugin || ':' || (confirmed_flush_lsn >= '${lsn}')::text::char, ',' order by slot_name collate \"C\") from pg_replication_slots where slot_name ~ '^${S}(_w[0-9]+)?$'")"
    check "${dir} publication count" 6 \
          "$(sql "${SRC}" -c "select count(*) from pg_publication where pubname ~ '^${S}_t[0-9]+$'")"
    check "${dir} publications" "$(expected_publications)" "$(published)"
    check "${dir} publication ${S}" 0 \
          "$(sql "${SRC}" -c "select count(*) from pg_publication where pubname = '${S}'")"
    check "${dir} publications of ww.ic" "$(sql "${SRC}" -c "select '${S}_t' || 'ww.ic'::regclass::oid")" \
          "$(sql "${SRC}" -c "select string_agg(pubname, ',') from pg_publication_tables where schemaname = 'ww' and tablename = 'ic'")"
    check "${dir} origins at ${lsn}" "${S}:t,${S}_w1:t,${S}_w2:t" \
          "$(sql "${TGT}" -c "select string_agg(roname || ':' || (pg_replication_origin_progress(roname, true) = '${lsn}')::text::char, ',' order by roname collate \"C\") from pg_replication_origin where roname ~ '^${S}(_w[0-9]+)?$'")"
    check "${dir} assignment" "$(expected_assignment)" "$(stored_assignment "${dir}")"
    check_worker_catalogs "${dir}"
}

# workers SIG log command...: runs command until its three WAL workers
# started, then SIG must stop them all and make it exit with an error
workers() {
    local sig=$1 log=$2 pid rc=0
    shift 2
    "$@" > "${log}" 2>&1 &
    pid=$!
    for _ in $(seq 240); do
        [ "$(grep -c 'Started WAL worker' "${log}")" -eq 3 ] && break
        kill -0 "${pid}" 2> /dev/null || break
        sleep 0.5
    done
    if [ "$(grep -c 'Started WAL worker' "${log}")" -ne 3 ]; then
        kill "${pid}" 2> /dev/null || true
        cat "${log}"
        echo "FAIL: three WAL workers did not start: $*" >&2
        exit 1
    fi
    kill "-${sig}" "${pid}"
    for _ in $(seq 120); do
        kill -0 "${pid}" 2> /dev/null || break
        sleep 0.5
    done
    if kill -0 "${pid}" 2> /dev/null; then
        pkill -KILL -f '^pgcopydb: ' 2> /dev/null || true
        kill -KILL "${pid}" 2> /dev/null || true
        cat "${log}"
        echo "FAIL: SIG${sig} did not stop $* within 60 s" >&2
        exit 1
    fi
    wait "${pid}" || rc=$?
    if [ "${rc}" -eq 0 ] || ! grep -qF 'stopping 3 WAL workers' "${log}"; then
        cat "${log}"
        echo "FAIL: SIG${sig} did not stop the WAL workers with an error, exit ${rc}: $*" >&2
        exit 1
    fi
    no_pgcopydb "a SIG${sig} to $*"
    wait_slots_inactive
    echo "OK workers started and stopped: $*"
}

# setup dir [SIG]
setup() {
    workers "${2:-TERM}" "$1.log" pgcopydb follow --dir "$1" --wal-workers 3 --wal-high-traffic-tables ww.k
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

# without the catalog, cleanup matches the names; a SIGQUIT stops the workers
setup /tmp/setup-2 QUIT
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

# vacuity guard: the same options pass the checks and re-use the catalogs
workers TERM /tmp/setup-3-same.log pgcopydb follow --resume --not-consistent --dir /tmp/setup-3 \
        --wal-workers 3 --wal-high-traffic-tables ww.k
grep -qF "Re-using replication slot \"${S}\"" /tmp/setup-3-same.log
if grep -qF 'Preparing the catalog of WAL worker' /tmp/setup-3-same.log; then
    echo "FAIL resume rebuilt a complete worker catalog" >&2
    exit 1
fi

sql "${SRC}" -c "select pg_drop_replication_slot('${S}_w2')" > /dev/null
expect "${SOURCE}" "Replication slot \"${S}_w2\" does not exist" /tmp/setup-3-slot.log \
       pgcopydb clone --follow --resume --dir /tmp/setup-3 --wal-workers 3 --wal-high-traffic-tables ww.k
check 'slot never created on resume' 0 \
      "$(sql "${SRC}" -c "select count(*) from pg_replication_slots where slot_name = '${S}_w2'")"

cleanup /tmp/setup-3 "Dropping replication slot \"${S}_w1\""

# a lost worker catalog is rebuilt only while the slot of that worker has not
# moved; a file in place of cdc/w2 stops the first run before any worker starts
mkdir -p /tmp/setup-4/cdc
touch /tmp/setup-4/cdc/w2
expect "${INTERNAL}" 'Failed to prepare WAL worker 2' /tmp/setup-4.log \
       pgcopydb follow --dir /tmp/setup-4 --wal-workers 3 --wal-high-traffic-tables ww.k
check 'slot of worker 1 where it was created' t \
      "$(sql "${SRC}" -c "select confirmed_flush_lsn = '$(slot_lsn /tmp/setup-4)' from pg_replication_slots where slot_name = '${S}_w1'")"

rm -f /tmp/setup-4/cdc/w2 /tmp/setup-4/cdc/w1/source.db*
workers TERM /tmp/setup-4-rebuild.log pgcopydb follow --resume --not-consistent --dir /tmp/setup-4 \
        --wal-workers 3 --wal-high-traffic-tables ww.k
check 'catalogs rebuilt on resume' '1 2' \
      "$(sed -n 's/.*Preparing the catalog of WAL worker \([0-9]\).*/\1/p' /tmp/setup-4-rebuild.log | paste -sd ' ')"
check_setup /tmp/setup-4

# once the slot moved, the changes the lost catalog had are gone for good
sql "${SRC}" -c "insert into ww.ic values (5000, 'moved')" \
             -c "select pg_replication_slot_advance('${S}_w1', pg_current_wal_lsn())" > /dev/null
check 'slot of worker 1 moved' t \
      "$(sql "${SRC}" -c "select confirmed_flush_lsn > '$(slot_lsn /tmp/setup-4)' from pg_replication_slots where slot_name = '${S}_w1'")"
rm -f /tmp/setup-4/cdc/w1/source.db*
expect "${INTERNAL}" "is missing or incomplete" /tmp/setup-4-moved.log \
       timeout 60 pgcopydb follow --resume --not-consistent --dir /tmp/setup-4 \
       --wal-workers 3 --wal-high-traffic-tables ww.k
test ! -e /tmp/setup-4/cdc/w1/source.db
no_pgcopydb 'a refused rebuild'

cleanup /tmp/setup-4 "Dropping replication slot \"${S}_w1\""

#
# convergence: three workers replay a concurrent write load exactly once
#
sql "${SRC}" -c "select pg_drop_replication_slot(slot_name) from pg_replication_slots" \
             -c "drop publication ${S}_t1x" \
             -c 'drop schema ww cascade' > /dev/null
sql "${TGT}" -c "select pg_replication_origin_drop(roname) from pg_replication_origin" > /dev/null
sql "${TGT}" > /dev/null <<'SQL'
drop schema if exists ww cascade;
select format('drop publication %I', pubname) from pg_publication
\gexec
SQL

sql "${SRC}" <<'SQL'
create schema cv;
create table cv.hot(id bigserial primary key, n bigint not null);
create table cv.acct(id integer primary key, balance bigint not null);
create table cv.evt(id bigint not null, kind text not null);
alter table cv.evt replica identity full;
create table cv.p(id integer not null, k integer not null, v text,
                  primary key (k, id)) partition by range (k);
create table cv.p1 partition of cv.p for values from (0) to (1000);
create table cv.p2 partition of cv.p for values from (1000) to (3000);
create table cv.g(id integer primary key, a integer not null,
                  b integer generated always as (a * 2) stored);
insert into cv.hot(n) select i from generate_series(1, 1000) i;
insert into cv.acct select i, 1000 from generate_series(0, 99) i;
insert into cv.evt select i, 'seed' from generate_series(-100, 0) i;
insert into cv.p select i, i, 'seed' from generate_series(0, 999) i;
insert into cv.g(id, a) select i, i from generate_series(1, 100) i;
SQL

CV_TABLES='cv.acct cv.evt cv.g cv.hot cv.p1 cv.p2'

# round i: one transaction across the tables of every worker
round() {
    local i=$1
    sql "${SRC}" -1 \
        -c "insert into cv.hot(n) values (${i})" \
        -c "update cv.hot set n = n + 1 where id = ${i}" \
        -c "update cv.acct set balance = balance - 1 where id = ${i} % 100" \
        -c "update cv.acct set balance = balance + 1 where id = (${i} + 1) % 100" \
        -c "insert into cv.evt values (${i}, 'ins'), (${i}, 'ins')" \
        -c "update cv.evt set kind = 'upd' where id = ${i} - 3" \
        -c "delete from cv.evt where id = ${i} - 7" \
        -c "update cv.p set k = k + 1000 where k = ${i} % 1000" \
        -c "insert into cv.p values (${i} + 100000, ${i} % 3000, 'w')" \
        -c "insert into cv.g(id, a) values (${i} + 100000, ${i})" \
        -c "update cv.g set a = a + 1 where id = ${i} % 100 + 1"
}

writer() {
    local i=0
    while [ ! -e /tmp/cv-stop ]; do
        i=$((i + 1))
        round "${i}"
    done
    echo "${i}" > /tmp/cv-rounds
}

counts() {
    local t out=''
    for t in ${CV_TABLES}; do
        out="${out} ${t}:$(sql "$1" -c "select count(*) || ':' || md5(coalesce(string_agg(x::text, ',' order by x::text), '')) from ${t} x")"
    done
    echo "${out# }"
}

rm -f /tmp/cv-stop /tmp/cv-rounds
writer &
writer_pid=$!

# the writer must be running before the clone starts
for _ in $(seq 60); do
    [ "$(sql "${SRC}" -c 'select count(*) from cv.hot')" -gt 1010 ] && break
    sleep 0.5
done
check 'writer running before the clone' t \
      "$(sql "${SRC}" -c 'select count(*) > 1010 from cv.hot')"

dir=/tmp/cv
log=/tmp/cv.log
env PGCOPYDB_WAL_WORKERS=3 PGCOPYDB_WAL_HIGH_TRAFFIC_TABLES=cv.hot \
    pgcopydb clone --follow --dir "${dir}" > "${log}" 2>&1 &
pid=$!

for _ in $(seq 240); do
    [ "$(grep -c 'Applying CDC changes' "${log}")" -eq 3 ] && break
    kill -0 "${pid}" 2> /dev/null || break
    sleep 0.5
done
touch /tmp/cv-stop
wait "${writer_pid}"
if [ "$(grep -c 'Applying CDC changes' "${log}")" -ne 3 ]; then
    kill "${pid}" 2> /dev/null || true
    cat "${log}"
    echo "FAIL convergence: the apply of three WAL workers did not start" >&2
    exit 1
fi
rounds=$(cat /tmp/cv-rounds)
echo "writer committed ${rounds} transactions"

worker_lsn() {
    sqlite3 -readonly -init /dev/null -batch -noheader "${dir}/cdc/w$1/source.db" \
            "select $2_lsn from sentinel"
}

# held what: t when the main sentinel holds the LSN of the paused worker 2,
# and workers 0 and 1, read before it, are past that LSN
held_at() {
    local main w0 w1 w2
    w0=$(worker_lsn 0 "$1")
    w1=$(worker_lsn 1 "$1")
    w2=$(worker_lsn 2 "$1")
    main=$(pgcopydb stream sentinel get --dir "${dir}" "--$1-lsn")
    sql "${SRC}" -c "select '${main}'::pg_lsn = '${w2}' and '${w2}'::pg_lsn < least('${w0}'::pg_lsn, '${w1}') and '${w2}'::pg_lsn > '0/0'"
}

applied() { echo "$(worker_lsn 0 replay) $(worker_lsn 1 replay)"; }

# moved before after: t when worker 0 or 1 applied something in between
moved() {
    local a b
    read -ra a <<< "$1"
    read -ra b <<< "$2"
    sql "${SRC}" -c "select '${a[0]}'::pg_lsn < '${b[0]}' or '${a[1]}'::pg_lsn < '${b[1]}'"
}

# fail_cv message: stop the clone and show its log
fail_cv() {
    kill "${pid}" 2> /dev/null || true
    cat "${log}"
    echo "FAIL convergence: $1" >&2
    exit 1
}

# the main replay_lsn leaves 0/0 once every worker applied a transaction
replaying=false
for _ in $(seq 120); do
    kill -0 "${pid}" 2> /dev/null || fail_cv 'clone --follow exited before every worker applied'
    if [ "$(sql "${SRC}" -c "select '$(pgcopydb stream sentinel get --dir "${dir}" --replay-lsn)'::pg_lsn > '0/0'")" = t ]; then
        replaying=true
        break
    fi
    sleep 0.5
done
${replaying} || fail_cv 'the main replay_lsn stayed at 0/0'

# the others move past a paused worker 2, the main sentinel stays with it; a
# process paused while it holds the catalog semaphore stalls all, so retry
wpid=$(pgrep -f '^pgcopydb: follow worker 2( |$)') || fail_cv 'no process for WAL worker 2'
paused=$(pgrep -P "${wpid}" -f '^pgcopydb: follow (receive|apply)( |$)' | paste -sd ' ') || true
[ "$(wc -w <<< "${paused}")" -eq 2 ] || fail_cv "receive and apply of worker 2: [${paused}]"
held=false
for _ in 1 2 3; do
    # shellcheck disable=SC2086
    kill -STOP ${paused}
    # receive syncs write and flush with its 10 s status message, and a pause
    # stays well below the 60 s wal_sender_timeout
    deadline=$((SECONDS + 25))
    since=''
    while [ "${SECONDS}" -lt "${deadline}" ]; do
        rounds=$((rounds + 1))
        round "${rounds}"
        if [ "$(held_at write)$(held_at flush)$(held_at replay)" != ttt ]; then
            since=''
        elif [ -z "${since}" ]; then
            since=${SECONDS}
            before=$(applied)
        # the supervisor syncs once a second, so a stale main sentinel passes
        # 3 s later only when the semaphore is stuck and nothing applies
        elif [ $((SECONDS - since)) -ge 4 ] && [ "$(moved "${before}" "$(applied)")" = t ]; then
            held=true
            break
        fi
        sleep 0.2
    done
    # shellcheck disable=SC2086
    kill -CONT ${paused}
    ${held} && break
    sleep 2
done
if ! ${held}; then
    for what in write flush replay; do
        echo "${what}: main $(pgcopydb stream sentinel get --dir "${dir}" "--${what}-lsn") workers $(worker_lsn 0 "${what}") $(worker_lsn 1 "${what}") $(worker_lsn 2 "${what}")"
    done
fi
check 'main sentinel held at the paused worker 2' true "${held}"

# the environment of the worker pod carries the worker options
expect 0 'endpos' /tmp/cv-endpos.log \
       env PGCOPYDB_WAL_WORKERS=3 PGCOPYDB_WAL_HIGH_TRAFFIC_TABLES=cv.hot \
       pgcopydb stream sentinel set endpos --current --dir "${dir}"

rc=124
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
    echo "FAIL convergence: clone --follow exit ${rc}" >&2
    exit 1
fi
no_pgcopydb 'the convergence run'
check 'workers reaching endpos' 3 "$(grep -c 'has reached endpos' "${log}")"
grep -qF 'Follow mode is now done, 3 WAL workers reached endpos' "${log}"
rc=0
grep -E ' (ERROR|FATAL) ' "${log}" || rc=$?
if [ "${rc}" -ne 1 ]; then
    echo "FAIL convergence: the run logged errors, grep exit ${rc}" >&2
    exit 1
fi

# vacuity guard: every worker replayed writes made after the slot creation
startpos=$(sqlite3 -readonly -init /dev/null -batch -noheader "${dir}/cdc/w0/source.db" 'select startpos from sentinel')
check 'workers that replayed changes' 3 \
      "$(sql "${TGT}" -c "select count(*) from pg_replication_origin_status where external_id ~ '^${S}(_w[0-9]+)?$' and remote_lsn > '${startpos}'")"

check 'per-table counts and contents' "$(counts "${SRC}")" "$(counts "${TGT}")"
check 'keyless duplicates kept' t \
      "$(sql "${TGT}" -c "select count(*) > count(distinct (id, kind)) from cv.evt")"
pgcopydb compare data --dir /tmp/cv-compare

# every table went to exactly one worker, cv.hot to a worker of its own
check 'worker of cv.hot' '0:1' \
      "$(sqlite3 -readonly -init /dev/null -batch -noheader "${dir}/schema/source.db" \
         "select worker || ':' || (select count(*) from wal_worker_table w where w.worker = t.worker) from wal_worker_table t where nspname = 'cv' and relname = 'hot'")"

# after the finish too, the main sentinel reports the lowest LSN of the workers
for what in write flush replay; do
    main=$(pgcopydb stream sentinel get --dir "${dir}" "--${what}-lsn")
    low=$(for k in 0 1 2; do
              sqlite3 -readonly -init /dev/null -batch -noheader "${dir}/cdc/w${k}/source.db" \
                      "select ${what}_lsn from sentinel"
          done | sed "s/.*/select '&'::pg_lsn as l/" | paste -sd ' ' | sed 's/ select/ union all select/g')
    check "main ${what}_lsn is the lowest of the workers" t \
          "$(sql "${SRC}" -c "select '${main}'::pg_lsn = min(l) and '${main}'::pg_lsn > '0/0' from (${low}) x")"
done

wait_slots_inactive
expect 0 "Dropping replication slot \"${S}_w2\"" /tmp/cv-cleanup.log \
       pgcopydb stream cleanup --dir "${dir}"

#
# failures: a killed apply stops every worker, a killed supervisor too
#
dir=/tmp/kill

# fail_kill message: stop the run and show its log
fail_kill() {
    kill -CONT "${pid}" 2> /dev/null || true
    kill "${pid}" 2> /dev/null || true
    cat "${log}"
    echo "FAIL: $1" >&2
    exit 1
}

# wait_exit: rc is the exit status of the run within 30 s, 124 while it runs
wait_exit() {
    rc=124
    for _ in $(seq 60); do
        if ! kill -0 "${pid}" 2> /dev/null; then
            rc=0
            wait "${pid}" || rc=$?
            return
        fi
        sleep 0.5
    done
}

# settled what: no pgcopydb process is left within 15 s
settled() {
    for _ in $(seq 30); do
        pgrep -x pgcopydb > /dev/null || break
        sleep 0.5
    done
    no_pgcopydb "$1"
}

# applying: t when worker 0 or 1 applies a new round within 30 s
applying() {
    local before
    before=$(applied)
    rounds=$((rounds + 1))
    round "${rounds}"
    for _ in $(seq 60); do
        [ "$(moved "${before}" "$(applied)")" = t ] && return 0
        sleep 0.5
    done
    return 1
}

# resume_workers log: follow --resume until the 3 WAL workers run
resume_workers() {
    log=$1
    pgcopydb follow --resume --not-consistent --dir "${dir}" --wal-workers 3 > "${log}" 2>&1 &
    pid=$!
    for _ in $(seq 240); do
        [ "$(pgrep -c -f '^pgcopydb: follow (receive|apply)' || true)" -eq 6 ] && return
        kill -0 "${pid}" 2> /dev/null || break
        sleep 0.5
    done
    fail_kill 'three WAL workers did not start on resume'
}

log=/tmp/kill-apply.log
pgcopydb follow --dir "${dir}" --wal-workers 3 > "${log}" 2>&1 &
pid=$!
for _ in $(seq 240); do
    [ "$(pgrep -c -f '^pgcopydb: follow apply' || true)" -eq 3 ] && break
    kill -0 "${pid}" 2> /dev/null || break
    sleep 0.5
done
apply=$(pgrep -f '^pgcopydb: follow apply' | head -1) || true
if [ -z "${apply}" ] || [ "$(pgrep -c -f '^pgcopydb: follow apply' || true)" -ne 3 ]; then
    fail_kill 'three apply processes did not start'
fi
kill -KILL "${apply}"
wait_exit
if [ "${rc}" -eq 0 ] || [ "${rc}" -eq 124 ] ||
   ! grep -qE 'WAL worker [0-9] with pid [0-9]+ has exited with code [0-9]+ and signal' "${log}"; then
    fail_kill "a killed apply left exit ${rc}"
fi
no_pgcopydb 'a killed apply'
echo "OK a killed apply stopped every worker, exit ${rc}"

# a WAL worker that exits 0 short of endpos stops the others; it gets no
# SIGTERM forwarded, so its receive and apply get one of their own
resume_workers /tmp/kill-short.log
wpid=$(pgrep -f '^pgcopydb: follow worker 1( |$)') || fail_kill 'no process for WAL worker 1'
kill -TERM "${wpid}"
pkill -TERM -P "${wpid}" || true
wait_exit
if [ "${rc}" -eq 0 ] || [ "${rc}" -eq 124 ] ||
   ! grep -qF "WAL worker 1 with pid ${wpid} has exited before reaching endpos" "${log}"; then
    fail_kill "a worker stopped short of endpos left exit ${rc}"
fi
settled 'a worker stopped short of endpos'
echo "OK a worker stopped short of endpos stopped every worker, exit ${rc}"

# a killed WAL worker leaves no receive or apply behind
resume_workers /tmp/kill-worker.log
wpid=$(pgrep -f '^pgcopydb: follow worker 1( |$)') || fail_kill 'no process for WAL worker 1'
apid=$(pgrep -P "${wpid}" -f '^pgcopydb: follow apply( |$)') || fail_kill 'no apply for WAL worker 1'
kill -KILL "${wpid}"
wait_exit
settled 'a killed WAL worker, within 15 s'
if [ "${rc}" -eq 0 ] || [ "${rc}" -eq 124 ] ||
   ! grep -qE " ${apid} INFO .*Apply process received a shutdown signal" "${log}"; then
    fail_kill "a killed WAL worker left exit ${rc}, or apply ${apid} got no signal"
fi
echo "OK a killed WAL worker stopped its receive and apply, exit ${rc}"

resume_workers /tmp/kill-supervisor.log
kill -KILL "${pid}"
wait "${pid}" || true
settled 'a killed supervisor, within 15 s'
echo "OK a killed supervisor stopped every worker"

# a signal is no finish: with endpos ahead, receive and apply stop on SIGTERM
# and no worker reaches endpos, while the paused supervisor holds one too
resume_workers /tmp/kill-stop.log
wpids=$(pgrep -f '^pgcopydb: follow worker [0-9]+( |$)' | paste -sd ' ') || true
[ "$(wc -w <<< "${wpids}")" -eq 3 ] || fail_kill "WAL workers: [${wpids}]"
pgcopydb stream sentinel set apply --dir "${dir}" > /tmp/kill-apply-on.log 2>&1 ||
    fail_kill 'stream sentinel set apply failed'
expect 0 'FF/0' /tmp/kill-endpos.log pgcopydb stream sentinel set endpos --dir "${dir}" FF/0
synced=false
for _ in $(seq 20); do
    ends=true
    for k in 0 1 2; do
        e=$(sqlite3 -readonly -init /dev/null -batch -noheader "${dir}/cdc/w${k}/source.db" \
                    "select coalesce(endpos, '0/0') from sentinel")
        ends="${ends} and '${e}'::pg_lsn = 'FF/0'"
    done
    if [ "$(sql "${SRC}" -c "select ${ends}")" = t ]; then
        synced=true
        break
    fi
    sleep 0.5
done
${synced} || fail_kill 'endpos did not reach the WAL workers'
applying || fail_kill 'the WAL workers did not apply'
# a supervisor paused while it holds the catalog semaphore stalls all, so retry
free=false
for _ in 1 2 3; do
    kill -STOP "${pid}"
    if applying; then
        free=true
        break
    fi
    kill -CONT "${pid}"
    sleep 1
done
${free} || fail_kill 'no worker applied while the supervisor was paused'
pkill -TERM -f '^pgcopydb: follow (receive|apply)( |$)' || true
zombies=0
for _ in $(seq 60); do
    # shellcheck disable=SC2086
    zombies=$(ps -o stat= -p ${wpids// /,} | grep -c Z || true)
    [ "${zombies}" -eq 3 ] && break
    sleep 0.5
done
[ "${zombies}" -eq 3 ] || fail_kill "WAL workers [${wpids}] did not exit, ${zombies} did"
kill -TERM "${pid}"
kill -CONT "${pid}"
wait_exit
settled 'a stop request'
if [ "${rc}" -eq 0 ] || [ "${rc}" -eq 124 ] ||
   [ "$(grep -c 'has reached endpos' "${log}")" -ne 0 ] ||
   ! grep -qF 'Follow mode stopped before every WAL worker reached endpos' "${log}"; then
    fail_kill "workers stopped by a signal left exit ${rc}"
fi
echo "OK workers stopped by a signal reach no endpos, exit ${rc}"

wait_slots_inactive
expect 0 "Dropping replication slot \"${S}_w2\"" /tmp/kill-cleanup.log \
       pgcopydb stream cleanup --dir "${dir}"
check 'objects left at the end' '0 0 0' \
      "$(sql "${SRC}" -c 'select count(*) from pg_replication_slots') $(sql "${SRC}" -c 'select count(*) from pg_publication') $(sql "${TGT}" -c 'select count(*) from pg_replication_origin')"
