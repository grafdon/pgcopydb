# Stage 0 probe: one slot, a raw spool, N apply workers

This harness measures three things before any C is written for a single-slot CDC path in the pgcopydb fork.
It drains one pgoutput slot with a trivial client (X1), analyses the row-level dependencies of the captured transactions and simulates worker schedules (X3), and replays the capture into a target with N worker processes that honour those dependencies (X4).

## Structure

```sh
probe/
├── README.md        # this file
├── Containerfile    # image probe-py: python 3.13 slim plus psycopg[binary]
├── lib.sh           # names, the shared podman lock, container helpers, the checksum query
├── probe.sh         # one step per call: setup, x1, index, x4, x3, teardown, selftest
├── run-full.sh      # the full matrix, resumable, ends with PROBE-FULL DONE or FAILED
├── smoke.sh         # the small matrix that proves the tooling
├── report.sh        # runs report.py in the probe-py image
├── report.py        # medians and comparison tables over a results directory
├── pgout.py         # pgoutput reader, dependency keys, pgcopydb's statement shapes
├── dispatch.py      # the dispatcher pass, the policies, the waits, the simulation (X3)
├── replay.py        # the parallel apply prototype (X4)
├── selftest.py      # reader and dispatcher checks on a hand-built capture
└── workload-m/      # small mixed workload for the paths A and H never take
```

Workload A comes from `../workload-a`, which the benchmark harness in `../harness` reads too.

## Getting started

1. Point `BENCH_LOCK` at the podman lock directory that every benchmark on this podman machine shares, and pick a results directory outside the repository.

    ```sh
    export BENCH_LOCK=/path/to/podman.lock OUT=/path/to/results
    mkdir -p "$OUT"
    ```

1. Run the selftest, then the smoke matrix, about 5 minutes.

    ```sh
    ./probe.sh "$OUT/probe" selftest
    ./smoke.sh "$OUT/smoke"
    ```

1. Read its tables.

    ```sh
    ./report.sh "$OUT/smoke"
    ```

> [!caution]
> Without a directory argument `smoke.sh`, `run-full.sh` and `report.sh` use `results-smoke` or `results-full` next to the scripts.
> The top-level `Dockerfile` copies `tests` into the build image, so keep results out of the repository.

## Commands

Start the full matrix in the background and wait for its last line.

```sh
OUT="$OUT/full" ./run-full.sh > "$OUT/run-full.out" 2>&1
./report.sh "$OUT/full"
```

We expect about 20 to 25 minutes for the full matrix.
The estimate is extrapolated from a run at a quarter of the size, which took 3.5 minutes with one replay per configuration.
`X4_REPS=1` gives one replay per configuration and about 10 minutes.
`OUT`, `A_TXNS` and `H_TXNS` change the results directory and the sizes; the transaction counts must be multiples of 16.
`PROBE_WINDOW` changes the affinity window for X3 and X4.

> [!important]
> Every script stops with a usage error when `BENCH_LOCK` is unset: a default lock would let two users of the podman machine run at once.
> Every step takes the shared podman lock, runs on fresh containers over the `probe-*` volumes, and removes its containers before it releases the lock.
> A step refuses to set up when the podman machine has less than 20 GiB free.
> The scripts create and remove only objects whose names start with `probe-`.

A rerun of `run-full.sh` skips every result that exists and is valid.
It continues an interrupted workload on the same backlog while the volumes of that setup still exist, and otherwise moves the old directory to `<workload>.prev-<epoch>` and starts the workload again.

## What a step does

`setup` starts a source and a target `postgres:18.7` with the benchmark harness's settings (source 4 CPUs, 6 GiB, no checkpoint and no autovacuum inside a measurement).
It seeds the source, copies the base state into the target database `probe_base` with `pg_dump`, creates the publication `probe` and K pgoutput slots while the source is quiet, and runs the pgbench backlog with no consumer.
It records `endpos` and the per-table checksums of the source.

`x1` drains one unused slot with `pg_recvlogical --start -o proto_version=1 -o publication_names=probe --endpos` into `/dev/null` or into a file on the `probe-work` volume.
The result holds the seconds around `pg_recvlogical`, the WAL bytes from the slot's `confirmed_flush_lsn` to `endpos`, the capture bytes and the source cgroup's CPU seconds.
The first file drain becomes `capture.bin`, the stand-in for the raw spool.

`index` is the dispatcher pass, see below.
It fails when the capture does not hold exactly the transactions pgbench ran.

`x4` copies `probe_base` into a fresh database, replays the capture with `replay.py`, and compares every table with the source by row count and by the sum of `hashtextextended(row::text, 0)`.
A result is `valid` only when the replay exited 0, every table is equal and no other container ran at the start or the end of the step.

`x3` fits the cost model on the first valid N = 1 replay at 8 CPUs and simulates every policy for the given worker counts.

## The design we prototyped

### Reader

`pgout.py` walks the capture message by message and expects the newline `pg_recvlogical` writes after each one.
It raises on an unknown message type, a missing newline, a change outside a transaction, a BEGIN without COMMIT and a capture that ends inside a transaction.

### Dispatcher

One pass over the capture, single-threaded and deterministic.
The sequence number of a transaction is its position in commit order.
The keys of a change are the relation plus the values of the replica identity columns, from the old tuple when there is one and from the new tuple.
With REPLICA IDENTITY FULL every column is an identity column, so the key is the whole tuple.
A transaction depends on the last earlier writer of each of its keys.
A TRUNCATE depends on everything before it, and everything after depends on it.
The index keeps, per transaction, its byte range in the capture, its row count, its commit LSN and time, and its dependencies.

### Policies

- `table`: every table belongs to one worker, heaviest table first onto the least-loaded worker, and a transaction goes whole to the worker of its first change.
- `rr`: sequence number modulo N.
- `affinity`: the worker that owns most of the recent dependencies, a tie to the lowest worker; with no recent dependency the worker with the fewest assigned rows.

We count rows queued on a worker as its time.
A dependency is recent when its place in its worker's queue, plus the window, is still ahead of the queue length of the least-loaded worker.
The window is 1000 rows (`PROBE_WINDOW`), a few group commits of margin.
X3 also simulates affinity at N = 8 for the windows 0, 100, 10000 and 100000.

### Waits

A transaction waits only for dependencies on another worker.
Per other worker we keep the highest dependency, and drop it when an earlier transaction of the same worker already waited that far.

### Worker

One OS process and one target connection per worker.
A worker decodes its own transactions from the capture by byte range, so decoding runs in parallel and counts as worker time.
Before a transaction with an unmet wait, the worker commits its open group, raises the other worker's `want` mark and sleeps on that worker's condition variable until its published progress reaches the dependency.
A worker publishes the sequence number of its last committed transaction after every group commit, in shared memory.
After every transaction a worker reads its own `want` mark and commits at once when a waiter needs a transaction of the open group; the result counts these as `forced_commits`.
With `--no-waits` the worker skips the waits, which is the negative control.

### Simulation

Each transaction costs `a + b * rows`.
A worker runs its transactions in order, and a transaction starts when its worker is free and every dependency on another worker has finished.
The report gives the makespan and speed-up per policy and N, the share of transactions with a dependency on another worker, the number of waits after the reduction above, the longest dependency chain and the critical path, which bounds the speed-up for any policy and any N.

## What we mirrored from pgcopydb

All of it comes from the fork export at 9d8b660.

- Session: `SET transaction_timeout TO 0`, then `pg_replication_origin_session_setup` on the worker's own origin, then pipeline mode (`ld_apply.c`, `setupReplicationOrigin`).
- Group open: `BEGIN` and the seven `SET` statements of `applySettingsSync`, with `synchronous_commit` on and `session_replication_role` replica, after every `BEGIN` (`ld_apply.c:38`, `stream_apply_group_open`).
- Group limits: 100 source transactions, 10000 rows or 200 ms, checked after each transaction (`ld_apply.c:603`, `ld_apply.c:1149`).
- Group commit: `set constraints all immediate`, `pg_replication_origin_xact_setup(lsn, timestamp)` of the last member, `commit` (`pgsql.c`, `pgsql_replication_origin_xact_commit`).
- A TRUNCATE gets a target transaction of its own (`stream_apply_txn_body`).
- INSERT: `INSERT INTO "s"."t" ("c", ...) overriding system value VALUES ($1, ...)` with no `ON CONFLICT`, as `stream_write_insert` writes it.
- UPDATE: `SET` holds the new columns whose old value is unknown or different, `WHERE` holds the identity columns, a NULL in `WHERE` is `IS NULL` (`stream_write_update`).
- An UPDATE without old tuple takes its key from the new tuple (`ld_pgoutput.c:761`).
- UPDATE and DELETE on a keyless table touch one row through `(tableoid, ctid) = (SELECT tableoid, ctid ... LIMIT 1)` (`rowsMayRepeat`).
- Statements are prepared once per text and executed with text parameters of unknown type.
- TRUNCATE: `TRUNCATE ONLY a, ONLY b [RESTART IDENTITY]` (`stream_write_truncate`).

## Deviations from the brief, and why

- The capture comes from `pg_recvlogical --start` with the pgoutput options; the plugin is bound when the setup creates the slot.
- The dependency map keeps the key bytes, not a hash, so REPLICA IDENTITY FULL has no collisions here; a C dispatcher would hash and gain only false dependencies.
- A relation without identity columns (for example `pgbench_history`) gets no keys, because it can only take INSERTs and those never conflict.
- "Recent" in `affinity` is measured in queued rows, not in sequence distance.
  A first version used a window of 4096 transactions; in one comparison at 320,000 transactions and N = 8 it needed 1.88 s against 1.36 s for this definition (N = 1: 4.87 s), because workers ran past the window and stalled behind a 50,000-row transaction.
  These were single runs.
- The commit statements go into the pipeline before the sync; pgcopydb syncs, leaves pipeline mode and sends them as one simple query, which is one round trip more per group.
- We do not merge consecutive INSERTs or DELETEs into one multi-row statement as `ld_transform.c` does; the workloads have at most three such rows in a row.
- The group's row limit counts changes; pgcopydb counts its merged statements.
- We take REPLICA IDENTITY FULL as the sign of a keyless table; pgcopydb takes the tables without a primary key or replica identity index from its catalog of the source.
- `run-full.sh` runs every X4 configuration three times, because one replay takes seconds and a single run is too noisy to compare.
- The base copy uses `pg_dump`, and the equality gate is our own SQL, because the probe has no pgcopydb in its containers.
- The client quota is 6 CPUs, so target and client together fit the 14 vCPUs of the podman machine.
- Containers are removed, not only stopped, between steps; the volumes keep the state.
- The cost fit runs over target transactions (seconds, source transactions, rows), since single statements have no time of their own in a pipeline.
  When rows and transactions are collinear, as in H, the fit gives `a` alone.
- Additions the brief did not ask for: workload M and `selftest.py` for correctness, the window sweep in X3, and the samples of `pg_stat_activity` that show what the workers' backends wait for.

## Known limits

- The client is Python.
  At N = 1 it took 1.8 s of CPU in a 4.9 s replay of 320,000 transactions, and a group's statements reach the target only at the sync or every 64 kB, so client and target work mostly one after the other.
  A C worker would make N = 1 faster; do not read the absolute numbers as the product's.
- A replay takes seconds.
  Connection start-up and the first use of each prepared statement are a visible share at smoke size; compare speed-ups only at full size and over the repetitions.
- The dispatcher runs before the workers start, so its time is outside the replay's wall seconds; `index.json` holds it as `index_s`.
- The simulation takes the commit that publishes a dependency as free, and the cost of a transaction as independent of N.
  In single runs at a quarter of the size it predicted 4.9x for affinity at N = 8 where the replay measured 3.3x, and the target used 1.6 times the CPU of N = 1 for the same work.
- Only row keys are tracked.
  Foreign keys are safe because `session_replication_role = replica` disables their triggers; a secondary unique index or an exclusion constraint could order two transactions that share no key, and the prototype would not see it.
- The per-worker origins do not give a restart point that is consistent across workers; the prototype has no restart and no retry.
- The last-writer map holds every key of the capture in memory.
- An unchanged TOAST column, a relation that changes inside the capture, and TRUNCATE of a table that is partitioned on the target are not supported; the first two stop the parse, see the `shortcut:` comments.
- DELETE, key changes, NULL identity values, duplicate keyless rows and TRUNCATE are exercised by workload M and the selftest only; A and H never produce them.
- The contention check looks for other running containers at the start and the end of a step, not in between.
- X1 uses the defaults of `pg_recvlogical`: status every 10 s, fsync of the file every 10 s and at the end.

## Result files

One directory per workload.
`x1-<sink>-<i>.json` holds one drain.
`index.json` holds the dispatcher statistics and rows per table.
`x3.json` and `x3.txt` hold the simulation.
`x4-<policy>-n<N>-c<target CPUs>[-nowaits][-r<i>].json` holds one replay: wall seconds from the common start of the workers to the last one done, transactions per second, per worker the busy and wait seconds, waits, forced commits, commits before a wait and CPU seconds, the target and client cgroup CPU seconds, the backend samples and the verdict (`ok`, `equal`, `tables_differ`, `contended`, `valid`).
Next to it are the replay log, the target's checksums and their diff against `source.sums`.
