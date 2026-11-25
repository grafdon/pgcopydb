"""X4: replay the captured transactions into the target with N worker processes.
Each worker applies whole source transactions in sequence order over one
connection in pipeline mode, and waits for the cross-worker dependencies the
dispatcher found.

Usage: replay.py CAPTURE INDEX DSN POLICY N OUT.json [--no-waits]
"""
import json
import multiprocessing
import multiprocessing.connection
import pickle
import sys
import time
from array import array
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg import pq

import dispatch
import pgout

# pgcopydb's group commit limits (ld_apply.c): transactions, rows, seconds
GROUP_TXNS, GROUP_ROWS, GROUP_S = 100, 10000, 0.2
# pgcopydb's applySettingsSync, sent after every BEGIN as it does
OPEN = [b"BEGIN"] + [b"SET %s TO %s" % g for g in (
    (b"client_encoding", b"'UTF-8'"), (b"extra_float_digits", b"3"), (b"statement_timeout", b"0"),
    (b"default_transaction_read_only", b"off"), (b"synchronous_commit", b"on"),
    (b"session_replication_role", b"'replica'"), (b"row_security", b"off"))]
PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)
SAMPLE_S = 0.05
SYNC, FATAL = pq.ExecStatus.PIPELINE_SYNC, pq.ExecStatus.FATAL_ERROR
ctx = multiprocessing.get_context("fork")


class Shared:
    def __init__(self, n):
        self.progress = ctx.RawArray("q", [-1] * n)  # last committed sequence number per worker
        self.want = ctx.RawArray("q", [-1] * n)  # highest sequence number a waiter needs per worker
        self.conds = [ctx.Condition() for _ in range(n)]
        self.abort = ctx.RawValue("i", 0)
        self.running = ctx.RawValue("i", 0)
        self.start = ctx.Barrier(n, action=self.go)  # all workers are connected and start together

    def go(self):
        self.running.value = 1


def worker(me, plan, capture, rels, dsn, sh, use_waits, keep_groups):
    try:
        apply(me, plan, capture, rels, dsn, sh, use_waits, keep_groups)
    except BaseException:
        sh.abort.value = 1
        sh.start.abort()
        raise


def apply(me, plan, capture, rels, dsn, sh, use_waits, keep_groups):
    buf = dispatch.open_capture(capture)
    progress, want, conds, abort = sh.progress, sh.want, sh.conds, sh.abort
    # libpq's blocking mode: a send that cannot proceed reads results
    pg = pq.PGconn.connect(dsn.encode() + b" application_name=probe-worker")
    if pg.status != pq.ConnStatus.OK:
        raise RuntimeError(pg.error_message.decode())
    # the session as pgcopydb's setupReplicationOrigin leaves it
    for sql in (b"SET transaction_timeout TO 0", b"select pg_replication_origin_session_setup('probe_w%d')" % me):
        r = pg.exec_(sql)
        if r.status not in (pq.ExecStatus.COMMAND_OK, pq.ExecStatus.TUPLES_OK):
            raise RuntimeError(r.error_message.decode())
    pg.enter_pipeline_mode()
    send, send_prepared, message, statement = pg.send_query_params, pg.send_query_prepared, pgout.message, pgout.statement
    prepared, shapes, groups = {}, {}, []
    st = dict(txns=0, rows=0, groups=0, waits=0, wait_s=0.0, forced_commits=0, commits_before_wait=0)
    g_open, g_txns, g_rows, g_start, g_seq, g_lsn, g_ts = False, 0, 0, 0.0, -1, 0, 0

    def commit():
        nonlocal g_open
        if abort.value:
            raise RuntimeError("another worker failed")
        # pgcopydb's commit: deferred constraints, origin progress, COMMIT
        send(b"set constraints all immediate", None)
        send(b"select pg_replication_origin_xact_setup($1, $2)", [
            b"%X/%X" % (g_lsn >> 32, g_lsn & 0xFFFFFFFF),
            (PG_EPOCH + timedelta(microseconds=g_ts)).isoformat().encode()])
        send(b"commit", None)
        pg.pipeline_sync()
        err, nulls = None, 0
        while True:
            r = pg.get_result()
            if r is None:  # the separator after each statement's result
                nulls += 1
                if nulls > 1:
                    raise RuntimeError("the pipeline ended before its sync")
                continue
            nulls = 0
            if r.status == SYNC:
                break
            if r.status == FATAL and err is None:
                err = r.error_message.decode(errors="replace")
        if err:
            raise RuntimeError(err)
        with conds[me]:
            progress[me] = g_seq
            conds[me].notify_all()
        g_open = False
        st["groups"] += 1
        if keep_groups:
            groups.append((g_txns, g_rows, time.monotonic() - g_start))

    sh.start.wait(120)
    t_start = time.monotonic()
    i, size = 0, len(plan)
    while i < size:
        seq, off, end, nrows, is_barrier, nwaits = plan[i:i + 6]
        i += 6
        for j in range(i, i + 2 * nwaits, 2):
            k, d = plan[j], plan[j + 1]
            if use_waits and progress[k] < d:
                # nothing of ours may stay uncommitted while we wait
                if g_open:
                    commit()
                    st["commits_before_wait"] += 1
                t0 = time.monotonic()
                with conds[k]:
                    if want[k] < d:
                        want[k] = d
                    while progress[k] < d:
                        if abort.value:
                            raise RuntimeError("another worker failed")
                        conds[k].wait(1.0)
                st["waits"] += 1
                st["wait_s"] += time.monotonic() - t0
        i += 2 * nwaits
        # a TRUNCATE gets a target transaction of its own, as in pgcopydb
        if is_barrier and g_open:
            commit()
        if not g_open:
            g_open, g_txns, g_rows, g_start = True, 0, 0, time.monotonic()
            for sql in OPEN:
                send(sql, None)
        p = off
        while p < end:
            p, tag, pl = message(buf, p, rels)
            if tag == pgout.INSERT or tag == pgout.UPDATE or tag == pgout.DELETE:
                stmt = statement(rels[pl[0]], tag, pl[1], pl[2], pl[3], shapes)
                if stmt is None:
                    continue
                name = prepared.get(stmt[0])
                if name is None:
                    name = prepared[stmt[0]] = b"p%d" % len(prepared)
                    pg.send_prepare(name, stmt[0].encode())
                send_prepared(name, stmt[1])
            elif tag == pgout.COMMIT:
                g_lsn, g_ts = pl[2], pl[3]
            elif tag == pgout.TRUNCATE:
                send(pgout.truncate_sql(rels, *pl).encode(), None)
        g_txns += 1
        g_rows += nrows
        g_seq = seq
        st["txns"] += 1
        st["rows"] += nrows
        if g_txns >= GROUP_TXNS or g_rows >= GROUP_ROWS or time.monotonic() - g_start >= GROUP_S or is_barrier:
            commit()
        elif progress[me] < want[me] <= seq:
            # another worker waits for a transaction of this open group
            commit()
            st["forced_commits"] += 1
    if g_open:
        commit()
    t_end = time.monotonic()
    pg.exit_pipeline_mode()
    pg.finish()
    st.update(start=t_start, end=t_end, elapsed_s=t_end - t_start, busy_s=t_end - t_start - st["wait_s"],
              cpu_s=time.process_time(), statements=len(prepared), groups_detail=groups)
    with open("/tmp/probe-w%d.json" % me, "w") as f:
        json.dump(st, f)


def cgroup_cpu():
    with open("/sys/fs/cgroup/cpu.stat") as f:
        return {k: int(v) for k, v in (line.split() for line in f)}


def main(argv):
    capture, index, dsn, policy, n, out = argv[0], argv[1], argv[2], argv[3], int(argv[4]), argv[5]
    use_waits = "--no-waits" not in argv
    with open(index, "rb") as f:
        idx = pickle.load(f)
    rels = dispatch.load_rels(idx)
    w = dispatch.assign(idx, policy, n)
    waits, crossing = dispatch.cross_waits(idx, w, n)
    ntx, nrows = len(w), sum(idx["rows"])
    # per worker a flat array: seq, begin, end, rows, barrier, waits, then (worker, seq) per wait
    plans = [array("q") for _ in range(n)]
    for s in range(ntx):
        plan = plans[w[s]]
        plan.extend((s, idx["off"][s], idx["end"][s], idx["rows"][s], idx["barrier"][s], len(waits[s])))
        for pair in waits[s]:
            plan.extend(pair)
    result = dict(policy=policy, n=n, waits_on=use_waits, window=dispatch.WINDOW, txns=ntx, rows=nrows,
                  cross_txn_share=crossing / ntx, planned_waits=sum(len(x) for x in waits))
    del idx, waits
    origins = ["probe_w%d" % k for k in range(n)]
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("select pg_replication_origin_drop(roname) from pg_replication_origin where roname = any(%s)", (origins,))
        for name in origins:
            c.execute("select pg_replication_origin_create(%s)", (name,))
    sh = Shared(n)
    cpu0 = cgroup_cpu()
    procs = [ctx.Process(target=worker, args=(k, plans[k], capture, rels, dsn, sh, use_waits, n == 1))
             for k in range(n)]
    for p in procs:
        p.start()
    # what the workers' backends do, sampled while the workers run: on CPU
    # ("active"), in a wait event, or idle because the client has sent nothing
    mon = psycopg.connect(dsn, autocommit=True)
    states = {}
    pending = set(procs)
    while pending:
        multiprocessing.connection.wait([p.sentinel for p in pending], timeout=SAMPLE_S)
        if sh.running.value:
            for state, kind, event in mon.execute("select state, wait_event_type, wait_event from pg_stat_activity"
                                                  " where application_name = 'probe-worker'"):
                key = state if kind is None or state != "active" else "%s:%s" % (kind, event)
                states[key] = states.get(key, 0) + 1
        for p in [p for p in pending if not p.is_alive()]:
            p.join()
            pending.discard(p)
            if p.exitcode != 0:
                sh.abort.value = 1  # the others stop at their next commit or wait
    cpu1 = cgroup_cpu()
    mon.close()
    result["backend_samples"] = states
    result["ok"] = all(p.exitcode == 0 for p in procs)
    result["client_cpu_s"] = (cpu1["usage_usec"] - cpu0["usage_usec"]) / 1e6
    result["client_throttled_s"] = (cpu1.get("throttled_usec", 0) - cpu0.get("throttled_usec", 0)) / 1e6
    if result["ok"]:
        workers = []
        for k in range(n):
            with open("/tmp/probe-w%d.json" % k) as f:
                workers.append(json.load(f))
        wall = max(x["end"] for x in workers) - min(x["start"] for x in workers)
        if n == 1:
            result["groups"] = workers[0]["groups_detail"]
        for x in workers:
            del x["start"], x["end"], x["groups_detail"]
        result.update(wall_s=wall, tps=ntx / wall, rows_per_s=nrows / wall, workers=workers)
        with psycopg.connect(dsn, autocommit=True) as c:
            c.execute("select pg_replication_origin_drop(roname) from pg_replication_origin where roname = any(%s)", (origins,))
    with open(out, "w") as f:
        json.dump(result, f)
    if not result["ok"]:
        raise SystemExit("replay failed, see the worker tracebacks above")
    print("%s N=%d: %.2f s, %.0f tps, waits %d (%.2f s), forced commits %d, commits before a wait %d" % (
        policy, n, wall, ntx / wall, sum(x["waits"] for x in workers), sum(x["wait_s"] for x in workers),
        sum(x["forced_commits"] for x in workers), sum(x["commits_before_wait"] for x in workers)))


if __name__ == "__main__":
    main(sys.argv[1:])
