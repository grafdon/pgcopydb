#! /bin/bash

set -euo pipefail

# With WAL workers the target gets no foreign key before every worker reached
# endpos, so a TRUNCATE of a referenced table applies on its own worker and a
# child row may land before its parent. Exit 0 means the foreign keys exist as
# on the source, a rerun creates them again only without the done file, and a
# stop short of endpos leaves them pending. One worker creates them in
# post-data as before. fk.k has no key, so a duplicate shows as a count.

SRC=${PGCOPYDB_SOURCE_PGURI}
TGT=${PGCOPYDB_TARGET_PGURI}
HT=fk.parent,fk.child

sql() { psql -X -q -At -v ON_ERROR_STOP=1 -d "$@"; }
check() { echo "${1}: want [${2}] got [${3}]"; test "${2}" = "${3}"; }

# pgrep exits 1 when nothing matches, and 2 or more when it failed to look
no_pgcopydb() {
    local rc=0
    pgrep -a -x pgcopydb || rc=$?
    if [ "${rc}" -ne 1 ]; then
        echo "FAIL: pgcopydb processes are left after $1, pgrep exit ${rc}" >&2
        exit 1
    fi
}

# settled what: no pgcopydb process is left within 15 s
settled() {
    for _ in $(seq 30); do
        pgrep -x pgcopydb > /dev/null || break
        sleep 0.5
    done
    no_pgcopydb "$1"
}

# fail message: stop the run and show its log
fail() {
    kill "${pid}" 2> /dev/null || true
    pkill -f '^pgcopydb: ' 2> /dev/null || true
    cat "${log}"
    echo "FAIL ${label}: $1" >&2
    exit 1
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

flush_lsn() { sql "${SRC}" -c 'select pg_current_wal_flush_lsn()'; }

FKEYS="from pg_constraint c where contype = 'f' and connamespace = 'fk'::regnamespace"

# fkeys uri: name, validity, enforcement and definition of each foreign key
fkeys() {
    sql "$1" -c "select string_agg(format('%s %s %s %s %s', conrelid::regclass, conname, convalidated, ${ENFORCED}, pg_get_constraintdef(oid)), E'\n' order by conrelid::regclass::text, conname) ${FKEYS}"
}

fkoids() { sql "$1" -c "select string_agg(oid::text, ' ' order by oid) ${FKEYS}"; }

nofk() { check "${label} foreign keys on the target $1" 0 "$(sql "${TGT}" -c "select count(*) ${FKEYS}")"; }

comment() { sql "$1" -c "select obj_description(oid, 'pg_constraint') ${FKEYS} and conname = 'child_parent_fk'"; }

digest() {
    local t out=''
    for t in fk.parent fk.child fk.ref fk.nv fk.ne fk.pc fk.k; do
        out="${out} ${t}=$(sql "$1" -c "select count(*) || ':' || md5(coalesce(string_agg(x::text, ',' order by x::text), '')) from ${t} x")"
    done
    echo "${out# }"
}

# applied what: the target holds the rows of the source within 60 s
applied() {
    for _ in $(seq 120); do
        [ "$(digest "${TGT}")" = "$(digest "${SRC}")" ] && return
        kill -0 "${pid}" 2> /dev/null || break
        sleep 0.5
    done
    fail "the target does not hold the rows of the source $1"
}

# wait_log count pattern: the log matches pattern count times within 60 s
wait_log() {
    for _ in $(seq 120); do
        [ "$(grep -cE "$2" "${log}" || true)" -ge "$1" ] && return
        kill -0 "${pid}" 2> /dev/null || break
        sleep 0.5
    done
    fail "the log does not match ${1} times: $2"
}

# finish secs: rc is the exit status of the run, 124 while it still runs
finish() {
    rc=124
    for _ in $(seq $(($1 * 2))); do
        if ! kill -0 "${pid}" 2> /dev/null; then
            rc=0
            wait "${pid}" || rc=$?
            return
        fi
        sleep 0.5
    done
}

# hold table: an open target transaction blocks writes to it, not reads,
# until release
hold() {
    PGAPPNAME=fk_holder psql -X -q -d "${TGT}" -v ON_ERROR_STOP=1 \
        -c 'begin' -c "lock table $1 in exclusive mode" \
        -c 'select pg_sleep(600)' > /dev/null 2>&1 &
    holder=$!
    for _ in $(seq 60); do
        [ "$(sql "${TGT}" -c "select count(*) from pg_locks l join pg_stat_activity a using (pid) where a.application_name = 'fk_holder' and l.mode = 'ExclusiveLock' and l.granted and l.relation = '$1'::regclass")" -eq 1 ] && return
        sleep 0.5
    done
    fail "no lock held on $1"
}

release() {
    sql "${TGT}" -c "select count(pg_terminate_backend(pid)) from pg_stat_activity where application_name = 'fk_holder'" > /dev/null
    wait "${holder}" || true
}

# waiting table: an apply session waits on the held lock within 30 s
waiting() {
    for _ in $(seq 60); do
        [ "$(sql "${TGT}" -c "select count(distinct pid) from pg_locks where not granted and relation = '$1'::regclass")" -eq 1 ] && return
        sleep 0.5
    done
    fail "no apply session waits on the lock held on $1"
}

# start label N list: a fresh clone --follow with N WAL workers, until every
# worker applies
start() {
    label=$1
    dir=/tmp/$1
    log=/tmp/$1.log

    sql "${TGT}" -c 'drop schema if exists fk cascade' > /dev/null

    pgcopydb clone --follow --dir "${dir}" --wal-workers "$2" \
             ${3:+--wal-high-traffic-tables "$3"} > "${log}" 2>&1 &
    pid=$!

    for _ in $(seq 240); do
        [ "$(grep -c 'Applying CDC changes' "${log}" || true)" -eq "$2" ] && break
        kill -0 "${pid}" 2> /dev/null || break
        sleep 0.5
    done
    [ "$(grep -c 'Applying CDC changes' "${log}" || true)" -eq "$2" ] ||
        fail "the apply of $2 WAL workers did not start"
}

# resume name: follow --resume of the last run, with its worker options
resume() {
    log=/tmp/${label}-$1.log
    pgcopydb follow --resume --not-consistent --dir "${dir}" \
             --wal-workers 3 --wal-high-traffic-tables "${HT}" > "${log}" 2>&1 &
    pid=$!
}

# cleanup: drop the slots, publications and origins of the last run, whose
# post-data restored no publication on the target
cleanup() {
    settled "${label}"
    wait_slots_inactive
    pgcopydb stream cleanup --dir "${dir}" > "${dir}-cleanup.log" 2>&1 ||
        { cat "${dir}-cleanup.log"; echo "FAIL ${label}: cleanup" >&2; exit 1; }
    check "${label} objects left after cleanup" '0 0 0 0' \
          "$(sql "${SRC}" -c 'select count(*) from pg_replication_slots') $(sql "${SRC}" -c 'select count(*) from pg_publication') $(sql "${TGT}" -c 'select count(*) from pg_replication_origin') $(sql "${TGT}" -c 'select count(*) from pg_publication')"
}

pgcopydb ping

# conenforced is new in Postgres 18
version=$(sql "${SRC}" -c 'show server_version_num')
if [ "${version}" -ge 180000 ]; then
    ENFORCED=conenforced
else
    ENFORCED="'-'"
fi

# fk.pc references fk.ref, so TRUNCATE fk.parent, fk.child runs on the source
sql "${SRC}" <<'SQL'
create schema fk;
create table fk.parent(id bigint primary key, v text);
create table fk.child(id bigint primary key, pid bigint, v text);
alter table fk.child add constraint child_parent_fk
      foreign key (pid) references fk.parent(id);
comment on constraint child_parent_fk on fk.child is 'a child needs a parent';
create table fk.ref(id bigint primary key);
create table fk.nv(id bigint primary key, rid bigint);
create table fk.ne(id bigint primary key, rid bigint);
create table fk.pc(id bigint, rid bigint references fk.ref(id)) partition by range (id);
create table fk.pc1 partition of fk.pc for values from (0) to (1000000);
create table fk.k(id bigint, v text);
insert into fk.parent select i, 'seed' from generate_series(1, 10) i;
insert into fk.child select i, i, 'seed' from generate_series(1, 10) i;
insert into fk.ref select i from generate_series(1, 10) i;
insert into fk.pc select i, i from generate_series(1, 10) i;
insert into fk.nv values (1, 1), (2, 999);
alter table fk.nv add constraint nv_ref_fk
      foreign key (rid) references fk.ref(id) not valid;
insert into fk.ne values (1, 999);
SQL

want=4
if [ "${ENFORCED}" = conenforced ]; then
    sql "${SRC}" -c 'alter table fk.ne add constraint ne_ref_fk foreign key (rid) references fk.ref(id) not enforced'
    want=5
fi

# fk.pc1 has its own copy of the foreign key of fk.pc
check 'foreign keys on the source' "${want}" "$(sql "${SRC}" -c "select count(*) ${FKEYS}")"
check 'NOT VALID foreign key with a violating row on the source' 'f 1' \
      "$(sql "${SRC}" -c "select convalidated ${FKEYS} and conname = 'nv_ref_fk'") $(sql "${SRC}" -c 'select count(*) from fk.nv where rid not in (select id from fk.ref)')"
if [ "${ENFORCED}" = conenforced ]; then
    check 'NOT ENFORCED foreign key on the source' f \
          "$(sql "${SRC}" -c "select conenforced ${FKEYS} and conname = 'ne_ref_fk'")"
fi
check 'comment on child_parent_fk on the source' 'a child needs a parent' "$(comment "${SRC}")"

#
# one WAL worker: post-data creates the foreign keys before apply, and a
# TRUNCATE of both tables applies as one statement
#
start single 1 ''
check 'single foreign keys on the target while following' "$(fkeys "${SRC}")" "$(fkeys "${TGT}")"
sql "${SRC}" -c 'truncate fk.parent, fk.child' \
             -c "insert into fk.parent select i, 'single' from generate_series(1, 20) i" \
             -c "insert into fk.child select i, i, 'single' from generate_series(1, 20) i" \
             -c "insert into fk.k values (1, 'single')"
pgcopydb stream sentinel set endpos --dir "${dir}" "$(flush_lsn)" > /dev/null
finish 120
[ "${rc}" -eq 0 ] || fail "clone --follow exit ${rc}"
check 'single tables' "$(digest "${SRC}")" "$(digest "${TGT}")"
check 'single foreign key files' 'none' \
      "$(ls "${dir}"/schema/fkeys* 2> /dev/null || echo none)"
cleanup
echo "OK one WAL worker creates the foreign keys in post-data"

#
# three WAL workers, fk.parent on worker 0 and fk.child on worker 1: both
# TRUNCATE forms apply, a child row lands while worker 0 is held back, and the
# foreign keys appear only after the last worker reached endpos
#
start main 3 "${HT}"
check 'assignment' '0:fk.parent 1:fk.child 2:fk.k 2:fk.ne 2:fk.nv 2:fk.pc 2:fk.pc1 2:fk.ref' \
      "$(sqlite3 -readonly -init /dev/null -batch -noheader "${dir}/schema/source.db" \
         "select group_concat(worker || ':' || nspname || '.' || relname, ' ') from (select * from wal_worker_table order by worker, relname)")"
nofk 'after post-data'

sql "${SRC}" -c 'truncate fk.parent, fk.child' \
             -c "insert into fk.parent select i, 'truncated' from generate_series(1, 20) i" \
             -c "insert into fk.child select i, i, 'truncated' from generate_series(1, 20) i"

# the source lists fk.child in the TRUNCATE of fk.parent ... CASCADE
sql "${SRC}" -c 'truncate fk.parent cascade' \
             -c "insert into fk.parent select i, 'cascaded' from generate_series(1, 30) i" \
             -c "insert into fk.child select i, i, 'cascaded' from generate_series(1, 30) i" \
             -c "insert into fk.k values (1, 'main')"
applied 'after the TRUNCATE statements'
nofk 'after the TRUNCATE statements'

hold fk.parent
sql "${SRC}" > /dev/null <<'SQL'
begin;
insert into fk.parent values (5001, 'held');
insert into fk.child values (5001, 5001, 'held');
commit;
SQL
waiting fk.parent
e=$(flush_lsn)
pgcopydb stream sentinel set endpos --dir "${dir}" "${e}" > /dev/null
wait_log 2 "WAL worker [12] with pid [0-9]+ has reached endpos ${e}\$"
check 'child and parent row on the target while worker 0 is held' '1 0' \
      "$(sql "${TGT}" -c 'select count(*) from fk.child where id = 5001') $(sql "${TGT}" -c 'select count(*) from fk.parent where id = 5001')"
nofk 'once workers 1 and 2 reached endpos'
kill -0 "${pid}" 2> /dev/null || fail 'clone --follow exited while worker 0 is held'
release

finish 120
[ "${rc}" -eq 0 ] || fail "clone --follow exit ${rc}"
check 'main tables' "$(digest "${SRC}")" "$(digest "${TGT}")"
check 'main foreign keys on the target' "$(fkeys "${SRC}")" "$(fkeys "${TGT}")"
check 'main comment on child_parent_fk' "$(comment "${SRC}")" "$(comment "${TGT}")"
grep -F 'Creating the foreign keys, every WAL worker reached endpos' "${log}" ||
    fail 'no foreign key pass'
test -f "${dir}/schema/fkeys.done" || fail 'no foreign key done file'
oids=$(fkoids "${TGT}")

resume rerun
finish 60
[ "${rc}" -eq 0 ] || fail "follow --resume exit ${rc}"
grep -F 'was previously reached' "${log}" || fail 'the rerun did not find endpos reached'
grep -F 'Skipping foreign keys, created on a previous run' "${log}" ||
    fail 'the rerun did not skip the foreign keys'
check 'main foreign keys after the rerun' "${oids}" "$(fkoids "${TGT}")"

# as after a stop between the foreign keys and their done file
rm "${dir}/schema/fkeys.done"
resume again
finish 60
[ "${rc}" -eq 0 ] || fail "follow --resume exit ${rc}"
grep -F 'Creating the foreign keys, every WAL worker reached endpos' "${log}" ||
    fail 'no foreign key pass without the done file'
[ "$(fkoids "${TGT}")" != "${oids}" ] || fail 'the foreign keys were not created again'
check 'main foreign keys created again' "$(fkeys "${SRC}")" "$(fkeys "${TGT}")"
check 'main comment created again' "$(comment "${SRC}")" "$(comment "${TGT}")"
test -f "${dir}/schema/fkeys.done" || fail 'no foreign key done file'
cleanup
echo "OK the foreign keys appear once every WAL worker reached endpos"

#
# a SIGTERM before endpos leaves the foreign keys pending, and the resume that
# reaches endpos creates them
#
start sigterm 3 "${HT}"
supervisor=$(pgrep -f '^pgcopydb: follow *$') || fail 'no follow supervisor'
kill -TERM "${supervisor}"
finish 60
[ "${rc}" -ne 0 ] && [ "${rc}" -ne 124 ] || fail "a SIGTERM left exit ${rc}"
grep -F 'Foreign keys are pending' "${log}" || fail 'no pending foreign keys warning'
nofk 'after a SIGTERM'
settled 'a SIGTERM'
wait_slots_inactive
echo "OK a SIGTERM leaves the foreign keys pending, exit ${rc}"

sql "${SRC}" -c "insert into fk.parent values (6001, 'resumed')" \
             -c "insert into fk.child values (6001, 6001, 'resumed')"
pgcopydb stream sentinel set endpos --dir "${dir}" "$(flush_lsn)" > /dev/null
resume resume
finish 120
[ "${rc}" -eq 0 ] || fail "follow --resume exit ${rc}"
check 'sigterm tables after the resume' "$(digest "${SRC}")" "$(digest "${TGT}")"
check 'sigterm foreign keys after the resume' "$(fkeys "${SRC}")" "$(fkeys "${TGT}")"
cleanup
echo "OK the resume after a SIGTERM creates the foreign keys"

#
# control: a foreign key on the target fails the TRUNCATE that worker 0
# applies on fk.parent alone
#
start control 3 "${HT}"
sql "${TGT}" -c 'alter table fk.child add constraint child_parent_fk foreign key (pid) references fk.parent(id)'
sql "${SRC}" -c 'truncate fk.parent, fk.child'
finish 60
if [ "${rc}" -eq 0 ] || [ "${rc}" -eq 124 ] ||
   ! grep -F 'cannot truncate a table referenced in a foreign key constraint' "${log}"; then
    fail "a foreign key on the target left exit ${rc}"
fi
grep -F 'Foreign keys are pending' "${log}" || fail 'no pending foreign keys warning'
cleanup
echo "OK a foreign key on the target fails the TRUNCATE of worker 0, exit ${rc}"

no_pgcopydb 'the suite'
