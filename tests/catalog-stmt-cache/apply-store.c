/*
 * Apply reads output.db and writes and reads replay.db once per source
 * transaction: each of those statements must be compiled once per connection,
 * end every call reset, and give back the values of the batch at hand.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "copydb.h"
#include "ld_store.h"
#include "lock_utils.h"

#define NTXNS 50
#define MAXSTMTS 16

static const char *outputFile = "/tmp/apply-store-output.db";
static const char *replayFile = "/tmp/apply-store-replay.db";
static bool failed = false;

/* a distinctive part of the text of each statement that apply runs */
static const char *outputSQL[] = {
	"e.action = 'C' and e.lsn > $1",
	"where action = 'C' and xid = $1",
	"where xid = $1 and id >= $2 and id <= $3",
	NULL
};

static const char *replaySQL[] = {
	"where action in ('C', 'R') and lsn > $1",
	"insert or ignore into stmt",
	"insert into replay",
	"left join stmt s on r.stmt_hash = s.hash",
	NULL
};

#define CHECK(cond, ...) \
	do { \
		if (!(cond)) { printf("FAIL: " __VA_ARGS__); printf("\n"); failed = true; } \
	} while (0)

#define MUST(call) \
	do { \
		if (!(call)) { printf("FAIL: %s\n", # call); exit(2); } \
	} while (0)


typedef struct Txn
{
	uint32_t xid;
	uint64_t lsn[4];            /* BEGIN, two DML rows, COMMIT */
	char timestamp[PG_MAX_TIMESTAMP];
	char json[2][64];
	uint32_t hash;
	char sql[64];
	char args[2][64];
} Txn;


/* every value depends on the batch, so one left from batch 0 shows in 1 */
static void
make_txn(Txn *txn, int batch, int t)
{
	int g = batch * NTXNS + t;

	memset(txn, 0, sizeof(Txn));
	txn->xid = 1000 + g;
	for (int k = 0; k < 4; k++)
	{
		txn->lsn[k] = (uint64_t) (batch + 1) * 0x1000000 + t * 16 + k + 1;
	}
	sformat(txn->timestamp, sizeof(txn->timestamp),
			"2026-11-25 00:%02d:%02d.000000+00", batch, t);
	sformat(txn->json[0], sizeof(txn->json[0]), "{\"a\":%d}", g);
	sformat(txn->json[1], sizeof(txn->json[1]), "{\"b\":%d}", g);
	txn->hash = 0x1000 + g;
	sformat(txn->sql, sizeof(txn->sql), "INSERT INTO t%d VALUES($1)", g);
	sformat(txn->args[0], sizeof(txn->args[0]), "[\"x%d\"]", g);
	sformat(txn->args[1], sizeof(txn->args[1]), "[\"y%d\"]", g);
}


/* checks that no statement is left mid-step, and lists the statements */
static int
list_stmts(sqlite3 *db, sqlite3_stmt **stmts, const char *after)
{
	int n = 0;

	for (sqlite3_stmt *s = sqlite3_next_stmt(db, NULL);
		 s != NULL;
		 s = sqlite3_next_stmt(db, s))
	{
		CHECK(!sqlite3_stmt_busy(s), "statement left mid-step after %s: %s",
			  after, sqlite3_sql(s));
		if (stmts != NULL && n < MAXSTMTS)
		{
			stmts[n] = s;
		}
		n++;
	}
	return n;
}


static void
check_idle(DatabaseCatalog *output, DatabaseCatalog *replay, const char *after)
{
	(void) list_stmts(output->db, NULL, after);
	(void) list_stmts(replay->db, NULL, after);
}


/* each statement is on the list once, and the list is saved for batch 1 */
static int
check_cached(sqlite3 *db, const char **texts, sqlite3_stmt **stmts)
{
	int n = list_stmts(db, stmts, "batch 0");

	for (int i = 0; texts[i] != NULL; i++)
	{
		int found = 0;

		for (int j = 0; j < n && j < MAXSTMTS; j++)
		{
			found += strstr(sqlite3_sql(stmts[j]), texts[i]) != NULL;
		}
		CHECK(found == 1, "statement with \"%s\" is %d times on the list "
						  "after batch 0, expected 1", texts[i], found);
	}
	return n;
}


static void
check_same_stmts(sqlite3 *db, sqlite3_stmt **expected, int n)
{
	sqlite3_stmt *stmts[MAXSTMTS];
	int count = list_stmts(db, stmts, "batch 1");

	CHECK(count == n, "%d prepared statements after batch 1, expected %d",
		  count, n);

	for (int i = 0; i < count && i < MAXSTMTS; i++)
	{
		bool found = false;
		for (int j = 0; j < n; j++)
		{
			found = found || stmts[i] == expected[j];
		}
		CHECK(found, "statement %s was compiled again in batch 1",
			  sqlite3_sql(stmts[i]));
	}
}


static void
insert_message(DatabaseCatalog *catalog, char action, Txn *txn, int k,
			   char *json)
{
	LogicalMessageMetadata metadata = { 0 };

	metadata.action = action;
	metadata.xid = txn->xid;
	metadata.lsn = txn->lsn[k];
	metadata.seq = k;
	metadata.jsonBuffer = json;
	strlcpy(metadata.timestamp, txn->timestamp, sizeof(metadata.timestamp));

	MUST(ld_store_insert_message(catalog, &metadata));
}


/* receive writes the output rows of the batch in one SQLite transaction */
static void
store_batch(DatabaseCatalog *output, Txn *txns)
{
	MUST(ld_store_output_begin(output));
	for (int t = 0; t < NTXNS; t++)
	{
		insert_message(output, 'B', &txns[t], 0, "{\"action\":\"B\"}");
		insert_message(output, 'I', &txns[t], 1, txns[t].json[0]);
		insert_message(output, 'I', &txns[t], 2, txns[t].json[1]);
		insert_message(output, 'C', &txns[t], 3, "{\"action\":\"C\"}");
	}
	MUST(ld_store_output_commit(output));
}


static void
check_output(ReplayDBOutputMessage *m, char action, Txn *txn, int k,
			 const char *what)
{
	CHECK(m->action == action && m->xid == txn->xid && m->lsn == txn->lsn[k],
		  "%s of xid %u: %c %u %llX, expected %c %u %llX", what, txn->xid,
		  m->action, m->xid, (unsigned long long) m->lsn,
		  action, txn->xid, (unsigned long long) txn->lsn[k]);
}


static void
insert_replay(DatabaseCatalog *replay, ReplayDBStmt *s)
{
	MUST(ld_store_insert_replay_stmt(replay, s));
}


/* the transform half of apply: read a transaction, write its replay rows */
static void
transform_txn(DatabaseCatalog *output, DatabaseCatalog *replay,
			  Txn *txn, uint64_t previousLSN)
{
	ReplayDBOutputMessage first = { 0 };
	ReplayDBOutputMessage last = { 0 };

	MUST(ld_store_lookup_output_after_lsn(output, previousLSN, previousLSN,
										  &first));
	check_output(&first, 'B', txn, 0, "next transaction");
	check_idle(output, replay, "ld_store_lookup_output_after_lsn");

	MUST(ld_store_lookup_output_xid_end(output, txn->xid, &last));
	check_output(&last, 'C', txn, 3, "COMMIT lookup");
	check_idle(output, replay, "ld_store_lookup_output_xid_end");

	ReplayDBOutputIterator iter = {
		.catalog = output,
		.transform_lsn = previousLSN,
		.keepalive_lsn = previousLSN
	};
	int k = 0;

	MUST(ld_store_iter_output_init(&iter));
	CHECK(iter.output != NULL, "output iterator of xid %u found nothing",
		  txn->xid);
	for (; iter.output != NULL && k < 5; k++)
	{
		MUST(ld_store_iter_output_next(&iter));
		if (iter.output == NULL)
		{
			break;
		}
		if (k < 4)
		{
			check_output(iter.output, "BIIC"[k], txn, k, "output iterator");
		}
		if (k == 1 || k == 2)
		{
			const char *json = iter.output->jsonBuffer;
			CHECK(json != NULL && strcmp(json, txn->json[k - 1]) == 0,
				  "output iterator of xid %u: message %s, expected %s",
				  txn->xid, json ? json : "NULL", txn->json[k - 1]);
		}
	}
	CHECK(k == 4, "output iterator of xid %u: %d rows, expected 4", txn->xid, k);
	MUST(ld_store_iter_output_finish(&iter));
	check_idle(output, replay, "the output iterator");

	ReplayDBStmt begin = {
		.action = 'B', .xid = txn->xid, .lsn = txn->lsn[0]
	};
	ReplayDBStmt dml = {
		.action = 'I', .xid = txn->xid, .lsn = txn->lsn[1], .hash = txn->hash,
		.stmt = txn->sql, .data = txn->args[0]
	};
	ReplayDBStmt known = {
		.action = 'I', .xid = txn->xid, .lsn = txn->lsn[2], .hash = txn->hash,
		.data = txn->args[1]
	};
	ReplayDBStmt keepalive = { .action = 'K' };
	ReplayDBStmt commit = {
		.action = 'C', .xid = txn->xid, .lsn = txn->lsn[3]
	};

	strlcpy(begin.timestamp, txn->timestamp, sizeof(begin.timestamp));
	strlcpy(commit.timestamp, txn->timestamp, sizeof(commit.timestamp));
	strlcpy(dml.nspname, "\"public\"", sizeof(dml.nspname));
	sformat(dml.relname, sizeof(dml.relname), "\"t%u\"", txn->xid);

	/* the keepalive row follows a row with values in every column */
	insert_replay(replay, &begin);
	insert_replay(replay, &dml);
	insert_replay(replay, &keepalive);
	insert_replay(replay, &known);
	insert_replay(replay, &commit);
	check_idle(output, replay, "ld_store_insert_replay_stmt");
}


static void
check_replay(ReplayDBStmt *s, char action, Txn *txn, int k, const char *what)
{
	CHECK(s->action == action && s->xid == txn->xid && s->lsn == txn->lsn[k],
		  "%s of xid %u: %c %u %llX, expected %c %u %llX", what, txn->xid,
		  s->action, s->xid, (unsigned long long) s->lsn,
		  action, txn->xid, (unsigned long long) txn->lsn[k]);
}


/* the apply half: find the transaction twice, then send its rows */
static void
apply_txn(DatabaseCatalog *output, DatabaseCatalog *replay,
		  Txn *txn, uint64_t previousLSN)
{
	ReplayDBStmt event = { 0 };

	for (int i = 0; i < 2; i++)
	{
		MUST(ld_store_replay_next_event(replay, previousLSN, previousLSN, &event));
		check_replay(&event, 'B', txn, 0, "next event");
		CHECK(strcmp(event.timestamp, txn->timestamp) == 0,
			  "next event of xid %u: timestamp %s, expected %s",
			  txn->xid, event.timestamp, txn->timestamp);
		check_idle(output, replay, "ld_store_replay_next_event");
	}

	ReplayDBReplayTxnIterator iter = {
		.catalog = replay, .xid = txn->xid, .begin_id = event.id
	};
	int k = 0;

	MUST(ld_store_iter_replay_txn_init(&iter));
	for (; k < 4; k++)
	{
		MUST(ld_store_iter_replay_txn_next(&iter));
		ReplayDBStmt *s = iter.current;

		CHECK(s != NULL, "replay iterator of xid %u ended before its COMMIT",
			  txn->xid);
		if (s == NULL)
		{
			break;
		}

		check_replay(s, "BIIC"[k], txn, k, "replay iterator");

		if (k == 1 || k == 2)
		{
			CHECK(s->stmt != NULL && strcmp(s->stmt, txn->sql) == 0 &&
				  s->hash == txn->hash,
				  "replay iterator of xid %u row %d: sql %s, expected %s",
				  txn->xid, k, s->stmt ? s->stmt : "NULL", txn->sql);
			CHECK(s->data != NULL && strcmp(s->data, txn->args[k - 1]) == 0,
				  "replay iterator of xid %u row %d: args %s, expected %s",
				  txn->xid, k, s->data ? s->data : "NULL", txn->args[k - 1]);
		}

		/* the apply loop stops at the COMMIT row, which leaves it mid-step */
		if (s->action == 'C')
		{
			break;
		}
	}
	CHECK(k == 3, "replay iterator of xid %u: COMMIT at row %d, expected 3",
		  txn->xid, k);
	MUST(ld_store_iter_replay_txn_finish(&iter));
	check_idle(output, replay, "the replay iterator");
}


/* the rows without an xid, lsn or timestamp keep NULL in every column */
static void
check_null_rows(DatabaseCatalog *replay, int batch)
{
	sqlite3_stmt *s = NULL;
	const char *sql =
		"select count(*), "
		"       count(xid) + count(lsn) + count(timestamp) + count(nspname) "
		"     + count(relname) + count(stmt_hash) + count(stmt_args) "
		"  from replay where action = 'K'";

	if (sqlite3_prepare_v2(replay->db, sql, -1, &s, NULL) != SQLITE_OK ||
		sqlite3_step(s) != SQLITE_ROW)
	{
		printf("FAIL: %s\n", sqlite3_errmsg(replay->db));
		exit(2);
	}

	int rows = sqlite3_column_int(s, 0);
	int values = sqlite3_column_int(s, 1);

	CHECK(rows == (batch + 1) * NTXNS, "%d keepalive rows after batch %d",
		  rows, batch);
	CHECK(values == 0, "keepalive rows hold %d values after batch %d",
		  values, batch);
	sqlite3_finalize(s);
}


/* returns the last COMMIT LSN of the batch, where the next one starts */
static uint64_t
run_batch(DatabaseCatalog *output, DatabaseCatalog *replay, int batch,
		  uint64_t previousLSN)
{
	Txn txns[NTXNS];

	for (int t = 0; t < NTXNS; t++)
	{
		make_txn(&txns[t], batch, t);
	}

	store_batch(output, txns);

	for (int t = 0; t < NTXNS; t++)
	{
		transform_txn(output, replay, &txns[t], previousLSN);
		apply_txn(output, replay, &txns[t], previousLSN);
		previousLSN = txns[t].lsn[3];
	}

	check_null_rows(replay, batch);

	return previousLSN;
}


static void
open_catalog(DatabaseCatalog *catalog, DatabaseCatalogType type, const char *file)
{
	(void) unlink(file);
	catalog->type = type;
	strlcpy(catalog->dbfile, file, sizeof(catalog->dbfile));

	if (!catalog_init(catalog))
	{
		printf("FAIL: could not create %s\n", file);
		exit(2);
	}
}


int
main(void)
{
	DatabaseCatalog *output = calloc(1, sizeof(DatabaseCatalog));
	DatabaseCatalog *replay = calloc(1, sizeof(DatabaseCatalog));
	sqlite3_stmt *outputStmts[MAXSTMTS];
	sqlite3_stmt *replayStmts[MAXSTMTS];

	setvbuf(stdout, NULL, _IOLBF, 0);

	/* the SQLite level logs every bind */
	log_set_level(LOG_INFO);

	open_catalog(output, DATABASE_CATALOG_TYPE_OUTPUT, outputFile);
	open_catalog(replay, DATABASE_CATALOG_TYPE_REPLAY, replayFile);

	uint64_t lsn = run_batch(output, replay, 0, InvalidXLogRecPtr);
	int nout = check_cached(output->db, outputSQL, outputStmts);
	int nrep = check_cached(replay->db, replaySQL, replayStmts);

	if (failed || nout > MAXSTMTS || nrep > MAXSTMTS)
	{
		printf("FAIL\n");
		return 1;
	}
	printf("ok: batch 0 applied with %d statements on output.db and %d on "
		   "replay.db\n", nout, nrep);

	(void) run_batch(output, replay, 1, lsn);
	check_same_stmts(output->db, outputStmts, nout);
	check_same_stmts(replay->db, replayStmts, nrep);
	printf("ok: batch 1 reused the same statements\n");

	CHECK(catalog_close(output), "catalog_close of output.db failed");
	CHECK(catalog_close(replay), "catalog_close of replay.db failed");
	(void) semaphore_finish(&output->sema);
	(void) semaphore_finish(&replay->sema);

	if (failed)
	{
		printf("FAIL\n");
		return 1;
	}

	printf("PASS: apply statements are compiled once and keep their values\n");
	return 0;
}
