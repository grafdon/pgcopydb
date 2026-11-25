"""pgoutput proto_version 1 reader for a pg_recvlogical capture, and the SQL
statements pgcopydb's ld_transform.c writes for a change.

pg_recvlogical writes each message followed by a newline and no length, so the
reader walks the self-delimiting messages and raises on any byte that does not fit.
"""
import struct

U16 = struct.Struct(">H").unpack_from
U32 = struct.Struct(">I").unpack_from
I32 = struct.Struct(">i").unpack_from
BEGIN_ = struct.Struct(">QqI").unpack_from  # final_lsn, commit_ts, xid
COMMIT_ = struct.Struct(">BQQq").unpack_from  # flags, commit_lsn, end_lsn, commit_ts

BEGIN, COMMIT, RELATION, INSERT, UPDATE, DELETE, TRUNCATE, TYPE, ORIGIN = b"BCRIUDTYO"
KEY, OLD, NEW = b"KON"
TEXT, NULL = b"tn"
TRUNCATE_RESTART_SEQS = 2


class Rel:
    """One RELATION message: quoted names, and which columns are the replica identity."""

    def __init__(self, relid, ns, name, replident, cols):
        self.relid, self.replident = relid, replident
        self.cols = [quote(c) for _, c in cols]
        self.keyidx = [i for i, (flags, _) in enumerate(cols) if flags & 1]
        self.name = quote(ns) + "." + quote(name)
        # pgcopydb's rowsMayRepeat: it knows the tables without a key from its
        # source catalog, the probe takes REPLICA IDENTITY FULL as that sign
        self.keyless = replident == "f"
        self.kprefix = struct.pack(">I", relid)
        self.insert = "INSERT INTO %s (%s) overriding system value VALUES (%s)" % (
            self.name, ", ".join(self.cols), ", ".join("$%d" % (i + 1) for i in range(len(cols))))
        self.sig = (ns, name, replident, tuple(cols))


def quote(ident):
    return '"' + ident.replace('"', '""') + '"'


def cstr(buf, p):
    e = buf.find(b"\0", p)
    if e < 0:
        raise ValueError("unterminated string at offset %d" % p)
    return e + 1, buf[p:e].decode()


def read_tuple(buf, p):
    """Column values at p as bytes, None for NULL; returns (next offset, values)."""
    (n,) = U16(buf, p)
    p += 2
    vals = []
    for _ in range(n):
        kind = buf[p]
        p += 1
        if kind == TEXT:
            (ln,) = I32(buf, p)
            p += 4
            vals.append(buf[p:p + ln])
            p += ln
        elif kind == NULL:
            vals.append(None)
        else:
            # shortcut: an unchanged TOAST column ('u') stops the parse, the
            # workloads have none; fill it from the old tuple when one is needed
            raise ValueError("column kind %r at offset %d is not supported" % (chr(kind), p - 1))
    return p, vals


def message(buf, p, rels):
    """Parse the message at p. Returns (offset after its newline, tag, payload).
    A change's payload is (relid, old kind or 0, old values or None, new values or None).
    RELATION messages fill rels and have no payload."""
    tag = buf[p]
    pl = None
    if tag == INSERT:
        (relid,) = U32(buf, p + 1)
        if buf[p + 5] != NEW:
            raise ValueError("INSERT without new tuple at offset %d" % p)
        q, new = read_tuple(buf, p + 6)
        pl = (relid, 0, None, new)
    elif tag == UPDATE:
        (relid,) = U32(buf, p + 1)
        q, kind, okind, old = p + 6, buf[p + 5], 0, None
        if kind == KEY or kind == OLD:
            okind = kind
            q, old = read_tuple(buf, q)
            kind = buf[q]
            q += 1
        if kind != NEW:
            raise ValueError("UPDATE without new tuple at offset %d" % p)
        q, new = read_tuple(buf, q)
        pl = (relid, okind, old, new)
    elif tag == DELETE:
        (relid,) = U32(buf, p + 1)
        okind = buf[p + 5]
        if okind != KEY and okind != OLD:
            raise ValueError("DELETE without old tuple at offset %d" % p)
        q, old = read_tuple(buf, p + 6)
        pl = (relid, okind, old, None)
    elif tag == BEGIN:
        pl = BEGIN_(buf, p + 1)
        q = p + 21
    elif tag == COMMIT:
        pl = COMMIT_(buf, p + 1)
        q = p + 26
    elif tag == RELATION:
        (relid,) = U32(buf, p + 1)
        q, ns = cstr(buf, p + 5)
        q, name = cstr(buf, q)
        replident = chr(buf[q])
        (natts,) = U16(buf, q + 1)
        q += 3
        cols = []
        for _ in range(natts):
            flags = buf[q]
            q, col = cstr(buf, q + 1)
            q += 8  # type oid and typmod
            cols.append((flags, col))
        rel = Rel(relid, ns or "pg_catalog", name, replident, cols)
        # shortcut: a relation that changes inside the capture stops the parse,
        # the workloads run no DDL; version the relation cache to support it
        if relid in rels and rels[relid].sig != rel.sig:
            raise ValueError("relation %s changed at offset %d" % (rel.name, p))
        rels[relid] = rel
    elif tag == TRUNCATE:
        (n,) = U32(buf, p + 1)
        pl = (buf[p + 5], struct.unpack_from(">%dI" % n, buf, p + 6))
        q = p + 6 + 4 * n
    elif tag == TYPE:
        q, _ = cstr(buf, p + 5)
        q, _ = cstr(buf, q)
    elif tag == ORIGIN:
        q, _ = cstr(buf, p + 9)
    else:
        raise ValueError("unknown message %r at offset %d" % (bytes([tag]), p))
    if q >= len(buf) or buf[q] != 10:
        raise ValueError("no newline after the message at offset %d" % p)
    return q + 1, tag, pl


def keys(rel, old, new):
    """Dependency keys of one change: relation plus the replica identity values of
    the old and the new tuple; REPLICA IDENTITY FULL flags every column. A relation
    without identity columns only takes INSERTs, which never conflict: no key."""
    ki = rel.keyidx
    out = []
    for tup in (old, new):
        if tup is not None and ki:
            # NUL cannot occur in a text value, \1 stands for NULL: a clash
            # with a real \1 value only adds a dependency
            out.append(rel.kprefix + b"\0".join(b"\1" if tup[i] is None else tup[i] for i in ki))
    return out


def statement(rel, tag, okind, old, new, cache):
    """(SQL, parameters) for one change as ld_transform.c writes it, or None for
    an UPDATE that changes nothing. cache maps a statement shape to its SQL."""
    if tag == INSERT:
        return rel.insert, new
    if old is None:  # key unchanged: pgcopydb builds the old key from the new tuple
        where = [(i, new[i]) for i in rel.keyidx]
    elif okind == KEY:  # the other columns of a key tuple are NULL placeholders
        where = [(i, v) for i, v in enumerate(old) if v is not None]
    else:
        where = list(enumerate(old))
    sets = ()
    if tag == UPDATE:
        was = dict(where)
        # a column whose old value is known and equal stays out of SET
        sets = tuple(i for i, v in enumerate(new) if not (i in was and was[i] == v))
        if not sets:
            return None
    shape = (rel.relid, tag, sets, tuple((i, v is None) for i, v in where))
    sql = cache.get(shape)
    if sql is None:
        n = len(sets)
        cond = []
        for i, v in where:
            if v is None:
                cond.append("%s IS NULL" % rel.cols[i])
            else:
                n += 1
                cond.append("%s = $%d" % (rel.cols[i], n))
        cond = " and ".join(cond)
        if rel.keyless:  # touch one row only: ctid is unique per partition, hence the tableoid
            cond = "(tableoid, ctid) = (SELECT tableoid, ctid FROM %s WHERE %s LIMIT 1)" % (rel.name, cond)
        if tag == UPDATE:
            head = "UPDATE %s SET %s" % (
                rel.name, ", ".join("%s = $%d" % (rel.cols[i], k + 1) for k, i in enumerate(sets)))
        else:
            head = "DELETE FROM %s" % rel.name
        sql = cache[shape] = head + " WHERE " + cond
    return sql, [new[i] for i in sets] + [v for _, v in where if v is not None]


def truncate_sql(rels, flags, relids):
    # shortcut: pgcopydb drops ONLY for a relation that is partitioned on the
    # target, the probe does not look that up; the workloads never truncate
    return "TRUNCATE " + ", ".join("ONLY " + rels[r].name for r in relids) + (
        " RESTART IDENTITY" if flags & TRUNCATE_RESTART_SEQS else "")
