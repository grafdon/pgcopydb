"""Medians and comparison tables over the result files of the probe.

Usage: report.py RESULTS_DIR   (one sub-directory per workload)
"""
import glob
import json
import os
import sys
from statistics import median

# medians of the release benchmark on workload A at 1,280,000 transactions (research brief)
REF_TXNS = 1280000
REF_RECEIVE = (("pgcopydb receive, 1 slot", 26.0, 7.3), ("pgcopydb receive, 8 slots", 15.3, 54.0))
REF_APPLY = (("pgcopydb apply, N = 1", 283.8), ("pgcopydb apply, N = 8", 133.5),
             ("pgcopydb apply, N = 8, high-traffic tables", 119.5))


def load(pattern):
    out = []
    for path in sorted(glob.glob(pattern)):
        with open(path) as f:
            out.append((os.path.basename(path), json.load(f)))
    return out


def table(head, rows):
    rows = [[("%.2f" % c if isinstance(c, float) else str(c)) for c in r] for r in rows]
    width = [max(len(r[i]) for r in [head] + rows) for i in range(len(head))]
    for r in [head] + rows:
        print("  " + "  ".join(c.ljust(w) if i == 0 else c.rjust(w) for i, (c, w) in enumerate(zip(r, width))))


def report(d):
    name = os.path.basename(d)
    x1, x4 = load(d + "/x1-*.json"), load(d + "/x4-*.json")
    if not x1 and not x4:
        return
    txns = (x1 or x4)[0][1].get("txns")
    full = name == "A" and txns == REF_TXNS
    print("\n== workload %s, %s transactions ==" % (name, txns))

    if x1:
        print("\nX1, one slot drained by pg_recvlogical (median of n runs)")
        rows = []
        for sink in ("null", "file"):
            runs = [r for _, r in x1 if r["sink"] == sink and not r["contended"]]
            if runs:
                rows.append([sink, len(runs), median(r["seconds"] for r in runs),
                             min(r["seconds"] for r in runs), max(r["seconds"] for r in runs),
                             runs[0]["wal_bytes"] / 1e6, runs[0]["capture_bytes"] / 1e6 if sink == "file" else "-",
                             median(r["wal_mb_per_s"] for r in runs), median(r["source_cpu_s"] for r in runs)])
        if full:
            rows += [[label, "-", s, "-", "-", "-", "-", "-", cpu] for label, s, cpu in REF_RECEIVE]
        table(["sink", "n", "seconds", "min", "max", "WAL MB", "capture MB", "WAL MB/s", "source CPU s"], rows)
        left = [f for f, r in x1 if r["contended"]]
        if left:
            print("  left out, another container ran: " + ", ".join(left))

    sim = {}
    if os.path.exists(d + "/x3.json"):
        with open(d + "/x3.json") as f:
            x3 = json.load(f)
        sim = {(r["policy"], r["n"]): r for r in x3["runs"]}
        print("\nX3, dependencies and simulated schedules")
        print("  cost %.1f us per transaction + %.1f us per row, fitted on %d target transactions of the N = 1 replay" % (
            x3["cost_a_s"] * 1e6, x3["cost_b_s"] * 1e6, x3["fit_groups"]))
        print("  longest dependency chain %d transactions, critical path %.2f s of %.2f s serial: speed-up bound %.2f" % (
            x3["longest_chain_txns"], x3["critical_path_s"], x3["total_cost_s"], x3["critical_path_speedup_bound"] or 0))
        head = ["policy", "N", "makespan s", "speed-up", "cross-worker txn %", "waits", "delayed txn %", "largest worker share"]
        line = lambda r: [r["makespan_s"], r["speedup"], 100 * r["cross_txn_share"], r["waits"],
                          100 * r["delayed_txns"] / x3["txns"], r["max_worker_share"]]
        table(head, [[r["policy"], r["n"]] + line(r) for r in x3["runs"]])
        if x3["sweep"]:
            print("  affinity at N = 8 by window in rows (the matrix above uses %d)" % x3["window"])
            table(["window"] + head[2:], [[r["window"]] + line(r) for r in x3["sweep"]])

    good = [(f, r) for f, r in x4 if r.get("valid")]
    for cpus in sorted({r["dst_cpus"] for _, r in good}):
        print("\nX4, parallel replay, target quota %d CPUs, client quota %d CPUs (median of n runs)" % (
            cpus, good[0][1]["client_cpus"]))
        runs = [r for _, r in good if r["dst_cpus"] == cpus]
        base = [r["wall_s"] for r in runs if r["n"] == 1]
        base = median(base) if base else None
        rows, states = [], []
        # N = 1 is the same schedule under every policy
        for policy, n in [("any", 1)] + sorted({(r["policy"], r["n"]) for r in runs if r["n"] > 1}):
            g = [r for r in runs if r["n"] == n and (n == 1 or r["policy"] == policy)]
            if not g:
                continue
            seen = {}
            for r in g:
                for key, count in r["backend_samples"].items():
                    kind = ("on CPU" if key == "active" else "client" if key.startswith(("idle", "Client:")) else
                            "WAL" if "WAL" in key.upper() else "lock" if key.startswith("Lock:") else "other wait")
                    seen[kind] = seen.get(kind, 0) + count
            states.append([policy, n, sum(seen.values())] + [
                100 * seen.get(k, 0) / max(1, sum(seen.values())) for k in ("on CPU", "WAL", "client", "lock", "other wait")])
            wall = median(r["wall_s"] for r in g)
            tot = lambda key: median(sum(w[key] for w in r["workers"]) for r in g)
            pred = sim.get((policy, n))
            rows.append([policy, n, len(g), wall, min(r["wall_s"] for r in g), max(r["wall_s"] for r in g),
                         median(r["tps"] for r in g), base / wall if base else "-",
                         pred["speedup"] if pred else "-", int(tot("waits")), tot("wait_s"),
                         int(tot("forced_commits")), int(tot("commits_before_wait")), int(tot("groups")),
                         median(r["dst_cpu_s"] for r in g), median(r["client_cpu_s"] for r in g),
                         median(r["client_throttled_s"] for r in g)])
        if full and cpus == 8:
            rows += [[label, "-", "-", s, "-", "-", txns / s] + ["-"] * 10 for label, s in REF_APPLY]
        table(["policy", "N", "n", "wall s", "min", "max", "tps", "speed-up", "X3 speed-up", "waits", "wait s",
               "forced commits", "commits before wait", "target txns", "target CPU s", "client CPU s",
               "client throttled s"], rows)
        print("  what the workers' target backends did, in percent of the samples; WAL: its locks, writes and syncs;"
              " client: the backend waits for the worker, which decodes or waits for a dependency")
        table(["policy", "N", "samples", "on CPU", "WAL", "client", "lock wait", "other wait"], states)
    bad = [(f, r) for f, r in x4 if not r.get("valid")]
    if bad:
        print("\nX4 runs that are NOT valid")
        table(["file", "replay ok", "equal to source", "tables that differ", "other containers ran"],
              [[f, r.get("ok"), r.get("equal"), r.get("tables_differ"), r.get("contended")] for f, r in bad])


def main(argv):
    for d in sorted(glob.glob(argv[0].rstrip("/") + "/*/")):
        if ".prev-" not in d:
            report(d.rstrip("/"))


if __name__ == "__main__":
    main(sys.argv[1:])
