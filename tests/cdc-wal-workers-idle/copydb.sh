#! /bin/bash

set -euo pipefail

# WAL worker 2 owns only tables that never change: its slot still advances
# while endpos is unset, a first insert into one of them that commits behind
# a large transaction of worker 0 after every worker knows endpos arrives,
# and the run exits within 120 s of endpos while the load goes on. id.ev has
# no key, so a row applied twice shows as a count.

SRC=${PGCOPYDB_SOURCE_PGURI}
TGT=${PGCOPYDB_TARGET_PGURI}
S=pgcopydb
dir=/tmp/idle
log=/tmp/idle.log
pid=
writer_pid=

on_exit() {
    local status=$?
    trap - EXIT
    touch /tmp/stop
    test -z "${writer_pid}" || wait "${writer_pid}" 2> /dev/null || true
    pkill -KILL -x pgcopydb 2> /dev/null || true
    exit "${status}"
}
trap on_exit EXIT

sql() { timeout 120s psql -X -q -At -v ON_ERROR_STOP=1 -d "$@"; }
fail() { tail -n 60 "${log}"; echo "FAIL: $*" >&2; exit 1; }
check() { echo "${1}: want [${2}] got [${3}]"; test "${2}" = "${3}" || fail "${1}"; }
lsn_gt() { sql "${SRC}" -c "select '$1'::pg_lsn > '$2'::pg_lsn"; }
confirmed() { sql "${SRC}" -c "select confirmed_flush_lsn from pg_replication_slots where slot_name = '$1'"; }

pgcopydb ping

sql "${SRC}" <<'SQL'
create schema id;
create table id.hot(id bigserial primary key, n bigint not null);
create table id.ev(i bigint not null, v text not null);
alter table id.ev replica identity full;
create table id.idle1(id integer primary key, v text);
create table id.idle2(id integer primary key, v text);
insert into id.hot(n) select i from generate_series(1, 100) i;
insert into id.ev select i, 'seed' from generate_series(1, 10) i;
insert into id.idle1 select i, 'seed' from generate_series(1, 10) i;
insert into id.idle2 select i, 'seed' from generate_series(1, 10) i;
SQL

# workers 0 and 1 take the loaded tables, worker 2 the two idle ones
rm -f /tmp/stop
(
    i=0
    while [ ! -e /tmp/stop ]; do
        i=$((i + 1))
        sql "${SRC}" -c "insert into id.hot(n) values (${i})" \
                     -c "insert into id.ev values (${i} + 1000, 'w'), (${i} + 1000, 'w')"
    done
) &
writer_pid=$!

pgcopydb clone --follow --dir "${dir}" --wal-workers 3 \
         --wal-high-traffic-tables id.hot,id.ev > "${log}" 2>&1 &
pid=$!
for _ in $(seq 240); do
    [ "$(grep -c 'Applying CDC changes' "${log}")" -eq 3 ] && break
    kill -0 "${pid}" 2> /dev/null || break
    sleep 0.5
done
[ "$(grep -c 'Applying CDC changes' "${log}")" -eq 3 ] || fail 'apply did not start'

check 'tables of worker 2' 'idle1,idle2' \
      "$(sqlite3 -readonly -init /dev/null -batch -noheader "${dir}/schema/source.db" \
         "select group_concat(relname, ',') from (select relname from wal_worker_table where worker = 2 order by relname)")"

# with endpos unset, the slot of worker 2 moves past where it was created
created=$(sqlite3 -readonly -init /dev/null -batch -noheader "${dir}/schema/source.db" \
          "select printf('%X/%X', lsn >> 32, lsn & 4294967295) from replication_slot")
first=
for _ in $(seq 120); do
    kill -0 "${pid}" 2> /dev/null || fail 'clone --follow exited'
    now=$(confirmed "${S}_w2")
    if [ "$(lsn_gt "${now}" "${created}")" = t ]; then
        if [ -z "${first}" ]; then
            first=${now}
        elif [ "$(lsn_gt "${now}" "${first}")" = t ]; then
            break
        fi
    fi
    sleep 1
done
echo "slot ${S}_w2: created at ${created}, then at ${first:-none} and ${now}"
test -n "${first}" || fail "slot ${S}_w2 never moved past its creation LSN"
check 'idle slot advanced twice' t "$(lsn_gt "${now}" "${first}")"
check 'endpos unset' 0/0 "$(pgcopydb stream sentinel get --endpos --dir "${dir}")"

# vacuity guard: worker 2 applied nothing, so its origin is still at creation
check 'origin of worker 2' "${created}" \
      "$(sql "${TGT}" -c "select pg_replication_origin_progress('${S}_w2', true)")"

# endpos goes ahead of the WAL first, so a worker that stops on a keepalive
# below endpos misses the late insert
pgcopydb stream sentinel set endpos --dir "${dir}" \
         "$(sql "${SRC}" -c "select pg_current_wal_lsn() + 256 * 1024 * 1024")" > /dev/null
endpos=$(pgcopydb stream sentinel get --endpos --dir "${dir}")
for _ in $(seq 60); do
    [ "$(grep -cF "endpos is now set to ${endpos}" "${log}")" -ge 3 ] && break
    sleep 0.5
done
[ "$(grep -cF "endpos is now set to ${endpos}" "${log}")" -ge 3 ] || fail 'the receives did not read endpos'

# a large transaction of worker 0 commits first, then the first insert into
# an idle table, both below endpos and under the load of workers 0 and 1
sql "${SRC}" -c "insert into id.hot(n) select -i from generate_series(1, 500000) i"
sql "${SRC}" -c "insert into id.idle1 values (100, 'late')"
hot=$(sql "${SRC}" -c 'select count(*) from id.hot')
ev=$(sql "${SRC}" -c 'select count(*) from id.ev')
check 'late insert below endpos' t "$(lsn_gt "${endpos}" "$(sql "${SRC}" -c 'select pg_current_wal_lsn()')")"
kill -0 "${pid}" 2> /dev/null || fail 'clone --follow exited before endpos'

# segment switches carry the WAL past endpos while the load goes on
for _ in $(seq 100); do
    [ "$(lsn_gt "$(sql "${SRC}" -c 'select pg_current_wal_lsn()')" "${endpos}")" = t ] && break
    sql "${SRC}" -c 'select pg_switch_wal()' > /dev/null
    sleep 0.2
done
check 'WAL past endpos' t "$(lsn_gt "$(sql "${SRC}" -c 'select pg_current_wal_lsn()')" "${endpos}")"
t0=${SECONDS}

rc=124
while [ $((SECONDS - t0)) -le 120 ]; do
    if ! kill -0 "${pid}" 2> /dev/null; then
        rc=0
        wait "${pid}" || rc=$?
        break
    fi
    sleep 0.5
done
elapsed=$((SECONDS - t0))
kill -0 "${writer_pid}" || fail 'the load stopped before the run exited'
touch /tmp/stop
wait "${writer_pid}"
writer_pid=
[ "${rc}" -eq 0 ] || fail "clone --follow exit ${rc} after ${elapsed} s"
echo "clone --follow exited ${elapsed} s after endpos ${endpos}"
grep -qF 'Follow mode is now done, 3 WAL workers reached endpos' "${log}" || fail 'not every worker reached endpos'

check 'late insert into an idle table' late "$(sql "${TGT}" -c 'select v from id.idle1 where id = 100')"
for t in id.idle1 id.idle2; do
    check "${t} contents" "$(sql "${SRC}" -c "select md5(string_agg(x::text, ',' order by x::text)) from ${t} x")" \
          "$(sql "${TGT}" -c "select md5(string_agg(x::text, ',' order by x::text)) from ${t} x")"
done
check 'hot rows committed before endpos' t "$(sql "${TGT}" -c "select count(*) >= ${hot} from id.hot")"
check 'keyless rows committed before endpos' t "$(sql "${TGT}" -c "select count(*) >= ${ev} from id.ev")"
check 'keyless rows applied once' 0 \
      "$(sql "${TGT}" -c "select count(*) from (select i from id.ev where i > 1000 group by i having count(*) <> 2) x")"
check 'origin of worker 2 at endpos' "${endpos}" \
      "$(sql "${TGT}" -c "select pg_replication_origin_progress('${S}_w2', true)")"

pgcopydb stream cleanup --dir "${dir}" > /tmp/idle-cleanup.log 2>&1 || { cat /tmp/idle-cleanup.log; fail 'cleanup'; }
check 'objects left' '0 0 0' \
      "$(sql "${SRC}" -c 'select count(*) from pg_replication_slots') $(sql "${SRC}" -c 'select count(*) from pg_publication') $(sql "${TGT}" -c 'select count(*) from pg_replication_origin')"
echo 'PASS: an idle WAL worker advances, receives a late insert and stops in time'
