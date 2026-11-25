"""Checks of the reader and the dispatcher on a hand-built capture, for the
paths the two workloads never take: DELETE, key tuples, NULL, TRUNCATE.

Usage: selftest.py
"""
import struct

import dispatch
import pgout


def tup(*vals):
    out = struct.pack(">H", len(vals))
    for v in vals:
        out += b"n" if v is None else b"t" + struct.pack(">i", len(v)) + v
    return out


def rel(relid, name, replident, *cols):
    out = b"R" + struct.pack(">I", relid) + b"public\0" + name + b"\0" + replident + struct.pack(">H", len(cols))
    for flags, col in cols:
        out += bytes([flags]) + col + b"\0" + struct.pack(">Ii", 25, -1)
    return out


def txn(*msgs):
    begin = b"B" + struct.pack(">QqI", 1, 2, 3)
    commit = b"C" + struct.pack(">BQQq", 0, 1, 0x100000002, 86400000000)
    return b"".join(m + b"\n" for m in (begin, *msgs, commit))


K, F = 1, 2  # relation oids: keyed on id, and REPLICA IDENTITY FULL
I = lambda r, *v: b"I" + struct.pack(">I", r) + b"N" + tup(*v)
CAPTURE = b"".join((
    txn(rel(K, b"k", b"d", (1, b"id"), (0, b"v")), I(K, b"1", b"a"), I(K, b"2", None)),          # 0
    txn(b"U" + struct.pack(">I", K) + b"N" + tup(b"1", b"b")),                                    # 1: after 0
    txn(b"U" + struct.pack(">I", K) + b"K" + tup(b"2", None) + b"N" + tup(b"3", b"c")),           # 2: after 0
    txn(I(K, b"2", b"again")),                                                                    # 3: after 2
    txn(b"D" + struct.pack(">I", K) + b"K" + tup(b"1", None)),                                    # 4: after 1
    txn(rel(F, b"f", b"f", (1, b"x"), (1, b"y")), I(F, b"p", None)),                              # 5: free
    txn(b"U" + struct.pack(">I", F) + b"O" + tup(b"p", None) + b"N" + tup(b"p", b"q")),           # 6: after 5
    txn(b"D" + struct.pack(">I", F) + b"O" + tup(b"p", b"q")),                                    # 7: after 6
    txn(b"T" + struct.pack(">IB", 2, 2) + struct.pack(">II", K, F)),                              # 8: barrier
    txn(I(K, b"9", b"z")),                                                                        # 9: after 8
))

idx = dispatch.build_index(CAPTURE)
deps = [list(idx["dep"][idx["dep_ptr"][s]:idx["dep_ptr"][s + 1]]) for s in range(10)]
assert deps == [[], [0], [0], [2], [1], [], [5], [6], [], [8]], deps
assert list(idx["rows"]) == [2, 1, 1, 1, 1, 1, 1, 1, 1, 1] and list(idx["barrier"]) == [0] * 8 + [1, 0]
assert idx["lsn"][0] == 0x100000002 and idx["rel_rows"] == {K: 7, F: 3}

rels, cache = dispatch.load_rels(idx), {}
got, p = [], 0
while p < len(CAPTURE):
    p, tag, pl = pgout.message(CAPTURE, p, rels)
    if tag in (pgout.INSERT, pgout.UPDATE, pgout.DELETE):
        got.append(pgout.statement(rels[pl[0]], tag, pl[1], pl[2], pl[3], cache))
    elif tag == pgout.TRUNCATE:
        got.append(pgout.truncate_sql(rels, *pl))
one = '(tableoid, ctid) = (SELECT tableoid, ctid FROM "public"."f" WHERE %s LIMIT 1)'
assert got == [
    ('INSERT INTO "public"."k" ("id", "v") overriding system value VALUES ($1, $2)', [b"1", b"a"]),
    ('INSERT INTO "public"."k" ("id", "v") overriding system value VALUES ($1, $2)', [b"2", None]),
    ('UPDATE "public"."k" SET "v" = $1 WHERE "id" = $2', [b"b", b"1"]),
    ('UPDATE "public"."k" SET "id" = $1, "v" = $2 WHERE "id" = $3', [b"3", b"c", b"2"]),
    ('INSERT INTO "public"."k" ("id", "v") overriding system value VALUES ($1, $2)', [b"2", b"again"]),
    ('DELETE FROM "public"."k" WHERE "id" = $1', [b"1"]),
    ('INSERT INTO "public"."f" ("x", "y") overriding system value VALUES ($1, $2)', [b"p", None]),
    ('UPDATE "public"."f" SET "y" = $1 WHERE ' + one % '"x" = $2 and "y" IS NULL', [b"q", b"p"]),
    ('DELETE FROM "public"."f" WHERE ' + one % '"x" = $1 and "y" = $2', [b"p", b"q"]),
    'TRUNCATE ONLY "public"."k", ONLY "public"."f" RESTART IDENTITY',
    ('INSERT INTO "public"."k" ("id", "v") overriding system value VALUES ($1, $2)', [b"9", b"z"]),
], got

for policy in dispatch.POLICIES:
    for n in (1, 2, 3):
        w = dispatch.assign(idx, policy, n)
        waits, crossing = dispatch.cross_waits(idx, w, n)
        assert all(0 <= k < n for k in w)
        # every dependency is on the same worker or covered by a wait at or before this transaction
        covered = [[-1] * n for _ in range(n)]
        for s in range(10):
            for k, d in waits[s]:
                assert k != w[s] and d < s
                covered[w[s]][k] = max(covered[w[s]][k], d)
            for d in deps[s]:
                assert w[d] == w[s] or covered[w[s]][w[d]] >= d, (policy, n, s, d)
            if idx["barrier"][s]:  # the TRUNCATE waits for the last transaction of every other worker
                assert all(covered[w[s]][w[e]] >= e for e in range(s) if w[e] != w[s]), (policy, n)
        sim = dispatch.simulate(idx, w, n, 1.0, 0.0)
        assert 10 / n <= sim["makespan_s"] <= 10, sim
assert dispatch.chains(idx, 1.0, 0.0) == (5, 5.0), dispatch.chains(idx, 1.0, 0.0)
assert dispatch.simulate(idx, dispatch.assign(idx, "rr", 1), 1, 1.0, 0.0)["makespan_s"] == 10
a, b = dispatch.fit_cost([(100, 250, 0.035), (100, 300, 0.04), (3, 10000, 1.0003)])
assert abs(a - 1e-4) < 1e-9 and abs(b - 1e-4) < 1e-9, (a, b)
a, b = dispatch.fit_cost([(100, 400, 0.02), (50, 200, 0.01)])
assert abs(a - 2e-4) < 1e-12 and b == 0.0, (a, b)

for bad in (CAPTURE[:-1], CAPTURE[:-27], b"X" + CAPTURE, CAPTURE.replace(b"\n", b"", 1), CAPTURE[21 + 1:]):
    try:
        dispatch.build_index(bad)
    except (ValueError, KeyError, IndexError, struct.error):
        continue
    raise AssertionError("a damaged capture parsed")
print("selftest ok")
