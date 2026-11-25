"""The offline dispatcher: one pass over the capture gives every transaction its
sequence number (commit order) and its dependencies. On top of that: the
assignment policies, the cross-worker waits and the schedule simulation (X3).

Usage: dispatch.py index CAPTURE INDEX STATS.json
       dispatch.py sim INDEX N1_RESULT.json OUT.json N[,N...]
"""
import json
import mmap
import os
import pickle
import sys
import time
from array import array

import pgout

POLICIES = ("table", "rr", "affinity")
# the affinity window in rows: the lead a worker must have over a dependency's
# place in its queue before the dependency counts as committed (see assign)
WINDOW = int(os.environ.get("PROBE_WINDOW", 1000))
SWEEP = (0, 100, 1000, 10000, 100000)


def open_capture(path):
    with open(path, "rb") as f:
        return mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)


def build_index(buf):
    """Arrays indexed by sequence number; deps are in CSR form (dep_ptr, dep)."""
    rels, rel_rows, last = {}, {}, {}
    off, end, rows, first = array("q"), array("q"), array("q"), array("q")
    lsn, ts, barrier = array("Q"), array("q"), array("b")
    dep_ptr, dep = array("q", [0]), array("q")
    p, size, open_ = 0, len(buf), False
    last_barrier = -1
    while p < size:
        q, tag, pl = pgout.message(buf, p, rels)
        if tag == pgout.INSERT or tag == pgout.UPDATE or tag == pgout.DELETE:
            if not open_:
                raise ValueError("change outside a transaction at offset %d" % p)
            relid, _, old, new = pl
            nrows += 1
            frel = frel or relid
            rel_rows[relid] = rel_rows.get(relid, 0) + 1
            wset.update(pgout.keys(rels[relid], old, new))
        elif tag == pgout.BEGIN:
            if open_:
                raise ValueError("BEGIN inside a transaction at offset %d" % p)
            open_, begin, wset, nrows, frel, trunc = True, p, set(), 0, 0, False
        elif tag == pgout.COMMIT:
            if not open_:
                raise ValueError("COMMIT without BEGIN at offset %d" % p)
            open_ = False
            seq = len(off)
            # a transaction depends on the last earlier writer of each of its keys
            deps = set()
            for k in wset:
                d = last.get(k)
                if d is not None:
                    deps.add(d)
                last[k] = seq
            # everything after a TRUNCATE depends on it
            if last_barrier >= 0:
                deps.add(last_barrier)
            if trunc:
                last_barrier = seq
            dep.extend(sorted(deps))
            dep_ptr.append(len(dep))
            off.append(begin)
            end.append(q)
            rows.append(nrows)
            first.append(frel)
            lsn.append(pl[2])
            ts.append(pl[3])
            barrier.append(trunc)
        elif tag == pgout.TRUNCATE:
            if not open_:
                raise ValueError("TRUNCATE outside a transaction at offset %d" % p)
            for relid in pl[1]:
                rels[relid]  # KeyError: truncated relation without RELATION message
            nrows, trunc, frel = nrows + 1, True, frel or pl[1][0]
        p = q
    if open_:
        raise ValueError("the capture ends inside a transaction")
    return dict(rels={r: x.sig for r, x in rels.items()}, rel_rows=rel_rows, off=off, end=end, rows=rows,
                first=first, lsn=lsn, ts=ts, barrier=barrier, dep_ptr=dep_ptr, dep=dep, keys=len(last))


def load_rels(idx):
    return {r: pgout.Rel(r, ns, name, ri, list(cols)) for r, (ns, name, ri, cols) in idx["rels"].items()}


def assign(idx, policy, n, window=WINDOW):
    """The worker of every transaction."""
    ntx, rows = len(idx["rows"]), idx["rows"]
    w = array("i", bytes(4 * ntx))
    load = [0] * n
    if policy == "rr":
        for s in range(ntx):
            w[s] = s % n
    elif policy == "table":
        # every table belongs to one worker, heaviest table first onto the
        # least-loaded worker; a transaction follows its first change
        owner = {}
        for relid, r in sorted(idx["rel_rows"].items(), key=lambda kv: (-kv[1], kv[0])):
            k = load.index(min(load))
            owner[relid] = k
            load[k] += r
        first = idx["first"]
        for s in range(ntx):
            w[s] = owner.get(first[s], 0)
    elif policy == "affinity":
        # rows queued on a worker stand for its time: a dependency is recent
        # when its place in its worker's queue, plus the window, is still
        # ahead of where the least-loaded worker stands
        ptr, dep = idx["dep_ptr"], idx["dep"]
        pos = array("q", bytes(8 * ntx))
        for s in range(ntx):
            low = min(load)
            votes = {}
            for j in range(ptr[s], ptr[s + 1]):
                d = dep[j]
                if pos[d] + window > low:
                    votes[w[d]] = votes.get(w[d], 0) + 1
            if votes:
                # a tie goes to the lowest worker, so a cluster of conflicting
                # rows gathers on one worker instead of bouncing between two
                k = max(sorted(votes), key=votes.get)
            else:
                k = load.index(low)
            w[s] = k
            load[k] += rows[s]
            pos[s] = load[k]
    else:
        raise ValueError("unknown policy %r" % policy)
    return w


def cross_waits(idx, w, n):
    """Per transaction the (worker, sequence number) pairs it must see committed
    first: per other worker the highest dependency, unless an earlier transaction
    of its worker waited that far. Also counts transactions with any such dependency."""
    ptr, dep, barrier = idx["dep_ptr"], idx["dep"], idx["barrier"]
    seen = [[-1] * n for _ in range(n)]  # seen[me][k]: how far me already waited on k
    lastseq = [-1] * n
    waits, crossing = [], 0
    none = ()
    for s in range(len(w)):
        me = w[s]
        best = {}
        for j in range(ptr[s], ptr[s + 1]):
            d = dep[j]
            k = w[d]
            if k != me and d > best.get(k, -1):
                best[k] = d
        if barrier[s]:  # a TRUNCATE depends on everything before it
            best = {k: lastseq[k] for k in range(n) if k != me and lastseq[k] >= 0}
        if best:
            crossing += 1
            mine = seen[me]
            out = [(k, d) for k, d in sorted(best.items()) if d > mine[k]]
            for k, d in out:
                mine[k] = d
            waits.append(out or none)
        else:
            waits.append(none)
        lastseq[me] = s
    return waits, crossing


def fit_cost(groups):
    """Least squares for seconds = a * transactions + b * rows over the target
    transactions of an N = 1 run. Falls back to a alone when the two are
    collinear (every transaction has the same row count) or a term turns negative."""
    snn = sum(g[0] * g[0] for g in groups)
    snr = sum(g[0] * g[1] for g in groups)
    srr = sum(g[1] * g[1] for g in groups)
    snt = sum(g[0] * g[2] for g in groups)
    srt = sum(g[1] * g[2] for g in groups)
    det = snn * srr - snr * snr
    if det > 1e-9 * snn * srr:
        a, b = (snt * srr - srt * snr) / det, (srt * snn - snt * snr) / det
        if a >= 0 and b >= 0:
            return a, b
    return sum(g[2] for g in groups) / sum(g[0] for g in groups), 0.0


def simulate(idx, w, n, a, b):
    """Makespan of the schedule: a worker runs its transactions in order, and a
    transaction starts once every dependency on another worker has finished.
    The commit that publishes a dependency is taken as free and instant."""
    ptr, dep, rows, barrier = idx["dep_ptr"], idx["dep"], idx["rows"], idx["barrier"]
    free = [0.0] * n
    busy = [0.0] * n
    fin = array("d", bytes(8 * len(w)))
    delayed, delay = 0, 0.0
    for s in range(len(w)):
        me = w[s]
        t = ready = free[me]
        for j in range(ptr[s], ptr[s + 1]):
            d = dep[j]
            if w[d] != me and fin[d] > ready:
                ready = fin[d]
        if barrier[s]:
            ready = max(free)
        if ready > t:
            delayed += 1
            delay += ready - t
        cost = a + b * rows[s]
        busy[me] += cost
        fin[s] = free[me] = ready + cost
    return dict(makespan_s=max(free), delayed_txns=delayed, delay_s=delay, max_worker_share=max(busy) / sum(busy))


def chains(idx, a, b):
    """Longest dependency chain in transactions, and the critical path in
    seconds: the makespan no worker count and no policy can beat."""
    ptr, dep, rows = idx["dep_ptr"], idx["dep"], idx["rows"]
    ntx = len(rows)
    length = array("q", bytes(8 * ntx))
    path = array("d", bytes(8 * ntx))
    barrier, top_len, top_path = idx["barrier"], 0, 0.0
    for s in range(ntx):
        ln, pt = 0, 0.0
        for j in range(ptr[s], ptr[s + 1]):
            d = dep[j]
            if length[d] > ln:
                ln = length[d]
            if path[d] > pt:
                pt = path[d]
        if barrier[s]:  # a TRUNCATE comes after everything before it
            ln, pt = top_len, top_path
        length[s] = ln + 1
        path[s] = pt + a + b * rows[s]
        top_len, top_path = max(top_len, length[s]), max(top_path, path[s])
    return top_len, top_path


def main(argv):
    if argv[0] == "index":
        t0 = time.monotonic()
        buf = open_capture(argv[1])
        idx = build_index(buf)
        with open(argv[2], "wb") as f:
            pickle.dump(idx, f, protocol=5)
        rels, ntx, ptr = load_rels(idx), len(idx["rows"]), idx["dep_ptr"]
        stats = dict(
            capture_bytes=len(buf), txns=ntx, rows=sum(idx["rows"]), max_txn_rows=max(idx["rows"], default=0),
            truncates=sum(idx["barrier"]), distinct_keys=idx["keys"], deps=len(idx["dep"]),
            txns_with_dep=sum(1 for s in range(ntx) if ptr[s + 1] > ptr[s]),
            rows_by_table={rels[r].name: c for r, c in sorted(idx["rel_rows"].items(), key=lambda kv: -kv[1])},
            index_s=round(time.monotonic() - t0, 3))
        with open(argv[3], "w") as f:
            json.dump(stats, f, indent=1)
        print(json.dumps({k: v for k, v in stats.items() if k != "rows_by_table"}))
    elif argv[0] == "sim":
        with open(argv[1], "rb") as f:
            idx = pickle.load(f)
        with open(argv[2]) as f:
            groups = json.load(f)["groups"]
        a, b = fit_cost(groups)
        rows, ntx = idx["rows"], len(idx["rows"])
        total = a * ntx + b * sum(rows)
        chain, critical = chains(idx, a, b)
        out = dict(
            cost_a_s=a, cost_b_s=b, fit_groups=len(groups), fit_measured_s=sum(g[2] for g in groups),
            total_cost_s=total, txns=ntx, rows=sum(rows), window=WINDOW, longest_chain_txns=chain,
            critical_path_s=critical, critical_path_speedup_bound=total / critical if critical else None,
            runs=[], sweep=[])
        print("cost per transaction %.1f us + %.1f us per row, serial %.2f s (N = 1 run measured %.2f s)" % (
            a * 1e6, b * 1e6, total, out["fit_measured_s"]))
        print("longest dependency chain %d of %d transactions, critical path %.2f s, speed-up bound %.2f" % (
            chain, ntx, critical, out["critical_path_speedup_bound"] or 0))
        print("%-9s %3s %7s %10s %8s %11s %9s %10s %9s" % (
            "policy", "N", "window", "makespan_s", "speedup", "cross_txn_%", "waits", "delayed_%", "max_share"))
        ns = [int(x) for x in argv[4].split(",")]
        # after the matrix: how the one parameter of affinity moves its result at 8 workers
        sweep = [("affinity", 8, win) for win in SWEEP if win != WINDOW and 8 in ns]
        for policy, n, win in [(p, n, WINDOW) for p in POLICIES for n in ns] + sweep:
            w = assign(idx, policy, n, win)
            waits, crossing = cross_waits(idx, w, n)
            r = simulate(idx, w, n, a, b)
            r.update(policy=policy, n=n, window=win, speedup=total / r["makespan_s"],
                     cross_txn_share=crossing / ntx, waits=sum(len(x) for x in waits))
            out["sweep" if win != WINDOW else "runs"].append(r)
            print("%-9s %3d %7s %10.2f %8.2f %11.2f %9d %10.2f %9.3f" % (
                policy, n, win if policy == "affinity" else "-", r["makespan_s"], r["speedup"],
                100 * r["cross_txn_share"], r["waits"], 100 * r["delayed_txns"] / ntx, r["max_worker_share"]))
        with open(argv[3], "w") as f:
            json.dump(out, f, indent=1)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
