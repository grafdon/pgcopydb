"""Split the samples of one pgcopydb process into exclusive groups by what the
call stack shows, from the text of `perf script -F time,period,ip,sym,dso`.
Every sample lands in exactly one group, so the shares add up to 100.

Usage: classify.py LABEL STACKS OUT.json
"""
import json
import subprocess
import sys
from collections import Counter

BINARY = "/usr/local/bin/pgcopydb"
PREPARE = {"sqlite3_prepare_v2", "sqlite3LockAndPrepare", "sqlite3Prepare", "sqlite3RunParser"}
WRAPPERS = ("catalog_sql_", "catalog_execute", "catalog_bind")
SEMAPHORE = ("semaphore_", "semop", "semtimedop", "__semtimedop")
EXPBUFFER = ("PQExpBuffer",)  # text building that lives in libpq.so
ALLOC = {"malloc", "free", "calloc", "realloc", "cfree", "_int_malloc", "_int_free", "_int_realloc", "int_malloc",
         "malloc_consolidate", "sysmalloc", "unlink_chunk.constprop.0", "__libc_malloc", "__libc_free"}
# which database a pgcopydb function works on, and how
OWNERS = (
    ("output.db write", ("ld_store_insert_pgoutput_message", "ld_store_insert_message", "ld_store_insert_internal",
                         "ld_store_output_begin", "ld_store_output_commit", "stream_commit_batch",
                         "ld_store_delete_output", "ld_store_insert_cdc", "ld_store_insert_timeline", "streamWrite",
                         "streamFlush", "streamCloseFile", "ld_store_rotate", "ld_store_close_outputdb")),
    ("output.db read", ("ld_store_lookup_output", "ld_store_iter_output", "ld_store_output_fetch",
                        "parsePgoutputMessage", "ld_store_cdc_filename", "ld_store_set_", "ld_store_open_outputdb",
                        "ld_store_read_last_commit_lsn", "stream_transform_from_outputdb")),
    ("replay.db write", ("ld_store_insert_replay_stmt", "stream_transform_write_replay", "ld_store_open_replaydb")),
    ("replay.db read", ("ld_store_replay_", "ld_store_iter_replay", "stream_apply_txn_body", "stream_apply_replaydb")),
    ("source.db (sentinel, pipeline state)", ("sentinel_", "pipeline_state", "stream_apply_sync_sentinel")),
)
TRANSFORM = ("stream_transform", "parsePgoutputMessage", "stream_write_", "streamLogicalTransaction", "fill_tuple",
             "ld_store_iter_output", "ld_store_output_fetch", "ld_store_insert_replay_stmt", "hashlittle",
             "FreeLogical", "AllocateLogical", "LogicalMessage", "stream_add_value", "coalesce", "canCoalesce",
             "markRowsMayRepeat", "quote_and_dup", "prepareGeneratedColumns", "stream_values")
APPLY = ("stream_apply", "ld_store_replay", "ld_store_iter_replay")
RECEIVE = ("streamWrite", "streamFlush", "streamKeepalive", "streamCloseFile", "prepareMessageMetadata",
           "parsePgoutputMessageActionAndXid", "preparePgoutputMessage", "decode_tuple", "pgout_", "pgoutput_",
           "pgsql_timestamptz_to_string", "pgsql_stream_logical", "startLogicalStreaming")
# pgcopydb's own functions, for the inclusive list
OWN = ("ld_", "stream", "catalog_", "sentinel_", "pipeline_", "pgsql_", "parsePgoutput", "preparePgoutput",
       "prepareMessage", "decode_tuple", "semaphore_", "json_", "fill_tuple", "follow_", "PQ", "pq", "GC_")
ROOTS = ("main", "cli_follow", "__libc_start", "_start", "commandline_run", "follow_")


def samples(path):
    """(time, period, [(ip, symbol, dso), ...] leaf first) per sample."""
    with open(path, errors="replace") as f:
        head, frames = None, []
        for line in f:
            if not line.strip():
                if head:
                    yield head[0], head[1], frames
                head, frames = None, []
            elif line[0] in " \t" and head and "(" in line:
                body, dso = line.strip().rsplit(" (", 1)
                ip, _, sym = body.partition(" ")
                frames.append((ip, sym.strip() or "[unknown]", dso.rstrip(")")))
            else:
                t, period = line.replace(":", " ").split()[:2]
                head = (float(t), int(period))
        if head:
            yield head[0], head[1], frames


def norm(sym):
    for prefix in ("__GI_", "__libc_", "__"):
        if sym.startswith(prefix):
            sym = sym[len(prefix):]
    return sym


def owner_of(frames, start):
    """Index of the first pgcopydb function rootward of frame start that is not a catalog wrapper."""
    for i in range(start + 1, len(frames)):
        _, sym, dso = frames[i]
        if dso == BINARY and not sym.startswith(WRAPPERS) and not sym.startswith("sqlite3"):
            return i
    return None


def database(frames, start):
    """The nearest caller that tells which database a statement runs on."""
    for _, sym, _ in frames[start:]:
        for name, prefixes in OWNERS:
            if sym.startswith(prefixes):
                return name
    return "other database"


def classify(frames):
    """(group, detail) of one stack."""
    syms = [s for _, s, _ in frames]
    api = [i for i, s in enumerate(syms) if s.startswith("sqlite3_")]
    if any(s in PREPARE for s in syms):
        own = owner_of(frames, max(i for i, s in enumerate(syms) if s in PREPARE or s.startswith("sqlite3_")))
        return "SQLite statement compilation", "%s@%s" % (frames[own][1], frames[own][0]) if own else "[no caller]@"
    if any(s.startswith(SEMAPHORE) or norm(s).startswith(SEMAPHORE) for s in syms):
        return "semaphores", ""
    if api:
        own = owner_of(frames, max(api))
        if own is None:
            return "SQLite execution: other database", "[no caller] via " + syms[max(api)]
        return "SQLite execution: " + database(frames, own), "%s via %s" % (syms[own], syms[max(api)])
    if any(s.startswith("json_") for s in syms):
        return "JSON build and parse", next(s for s in reversed(syms) if s.startswith("json_"))
    # appendPQExpBuffer and friends live in libpq.so but are text building of their caller
    text = any("PQExpBuffer" in s for s in syms)
    libpq = [s for _, s, d in frames if "libpq" in d or (s.startswith("pgsql_") and not s.startswith(RECEIVE))]
    if libpq and not text:
        return "libpq and network", libpq[-1]
    if not text and any(s.startswith("GC_") or "libgc" in d or norm(s) in ALLOC for _, s, d in frames):
        return "allocator and GC", ""
    if any(s.startswith(TRANSFORM) for s in syms):
        return "transform's own work", next(s for s in syms if s.startswith(TRANSFORM))
    if any(s.startswith(APPLY) for s in syms):
        return "apply loop's own work", next(s for s in syms if s.startswith(APPLY))
    if any(s.startswith(RECEIVE) for s in syms):
        return "receive's own work (pgoutput parse, metadata)", next(s for s in syms if s.startswith(RECEIVE))
    return "everything else", syms[0]


def main(label, stacks, out):
    total = kernel = 0
    groups, details, selfs, kern, incl = Counter(), Counter(), Counter(), Counter(), Counter()
    unrooted, gself, entry = Counter(), Counter(), Counter()
    seconds = Counter()
    for t, period, frames in samples(stacks):
        if not frames:
            continue
        total += period
        group, detail = classify(frames)
        groups[group] += period
        details[(group, detail)] += period
        selfs[(frames[0][1], frames[0][2].rsplit("/", 1)[-1])] += period
        if "kernel" in frames[0][2]:
            kernel += period
            kern[group] += period
            # where the process entered the kernel: a system call wrapper or a faulting function
            first = next((norm(s) for _, s, d in frames if "kernel" not in d), "[no user frame]")
            entry[(group, first)] += period
        gself[(group, frames[0][1])] += period
        for sym in {s for _, s, _ in frames}:
            incl[sym] += period
        if not any(s.startswith(ROOTS) for _, s, _ in frames):
            unrooted[group] += period
        # a cut stack could hide its SQLite entry point and land in a wrong group
        if not group.startswith("SQLite") and any(s.startswith("sqlite3") for _, s, _ in frames):
            unrooted["SQLite frames outside the SQLite groups"] += period
        seconds[int(t)] += period
    pct = lambda n: 100.0 * n / total
    # call sites of the compilations: the return address minus one instruction
    lines = {}
    for (group, detail), _ in details.items():
        if group == "SQLite statement compilation" and "@" in detail and detail.split("@")[1]:
            ip = detail.split("@")[1]
            got = subprocess.run(["addr2line", "-e", BINARY, hex(int(ip, 16) - 4)], capture_output=True, text=True)
            lines[detail] = got.stdout.strip().rsplit("/", 1)[-1]
    compiled = Counter()
    for (group, detail), n in details.items():
        if group == "SQLite statement compilation":
            compiled["%s (%s)" % (detail.split("@")[0], lines.get(detail, "?"))] += n

    print("%s: %d s of sampled CPU in %d samples, kernel %.1f %%, user %.1f %%" % (
        label, round(total / 1e9), sum(1 for _ in samples(stacks)), pct(kernel), 100 - pct(kernel)))
    print("\nexclusive groups, percent of all samples (of which in the kernel)")
    for group, n in groups.most_common():
        print("  %5.1f  (%4.1f)  %s" % (pct(n), pct(kern[group]), group))
    print("  %5.1f          total" % pct(sum(groups.values())))
    print("\nstatement compilation by call site, percent of all samples")
    for site, n in compiled.most_common():
        print("  %5.1f  %s" % (pct(n), site))
    print("\nlargest details of the other groups, percent of all samples")
    for (group, detail), n in details.most_common():
        if group != "SQLite statement compilation" and detail and pct(n) >= 0.3:
            print("  %5.1f  %-38s %s" % (pct(n), group, detail))
    print("\ntop 25 symbols by self time, percent of all samples")
    for (sym, dso), n in selfs.most_common(25):
        print("  %5.1f  %-44s %s" % (pct(n), sym, dso))
    print("\nkernel time by group and by the user function that entered the kernel, percent of all samples")
    for (group, first), n in entry.most_common(22):
        print("  %5.1f  %-38s %s" % (pct(n), group, first))
    print("\nlargest self times inside each group, percent of all samples")
    for group, _ in groups.most_common():
        top = [(sym, n) for (g, sym), n in gself.most_common() if g == group][:6]
        print("  %s: %s" % (group, ", ".join("%s %.1f" % (sym, pct(n)) for sym, n in top)))
    print("\ninclusive shares of pgcopydb's own functions and library entry points, top 40 (they overlap)")
    for sym, n in [(sym, n) for sym, n in incl.most_common() if sym.startswith(OWN)][:40]:
        print("  %5.1f  %s" % (pct(n), sym))
    print("\ninclusive shares of selected functions, percent of all samples (they overlap)")
    for sym in ("catalog_sql_prepare", "catalog_sql_prepare_cached", "sqlite3_prepare_v2", "sqlite3_step",
                "sqlite3_finalize", "sqlite3_exec", "stream_transform_from_outputdb", "stream_apply_txn_body",
                "ld_store_replay_next_event", "ld_store_lookup_output_after_lsn", "ld_store_lookup_output_xid_end",
                "ld_store_iter_output_init", "ld_store_insert_replay_stmt", "ld_store_iter_replay_txn_init",
                "sentinel_get", "parsePgoutputMessage", "json_serialize_to_string", "json_parse_string",
                "PQsendQueryPrepared", "pgsql_sync_pipeline", "semaphore_lock", "semaphore_unlock", "GC_malloc"):
        print("  %5.1f  %s" % (pct(incl[sym]), sym))
    print("\nstacks that do not reach the follow loop, percent of all samples, by the group they landed in")
    for group, n in unrooted.most_common():
        print("  %5.1f  %s" % (pct(n), group))
    with open(out, "w") as f:
        json.dump(dict(label=label, sampled_cpu_s=total / 1e9, kernel_pct=pct(kernel),
                       groups={g: pct(n) for g, n in groups.most_common()},
                       groups_kernel={g: pct(n) for g, n in kern.most_common()},
                       compile_sites={s: pct(n) for s, n in compiled.most_common()},
                       top_self=[[s, d, pct(n)] for (s, d), n in selfs.most_common(40)],
                       inclusive={s: pct(n) for s, n in incl.most_common(150)},
                       unrooted={g: pct(n) for g, n in unrooted.items()},
                       kernel_entry=[[g, e, pct(n)] for (g, e), n in entry.most_common(60)],
                       per_second={str(k): v / 1e9 for k, v in sorted(seconds.items())}), f, indent=1)


if __name__ == "__main__":
    main(*sys.argv[1:4])
