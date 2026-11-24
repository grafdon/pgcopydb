/*
 * Receive stores pgoutput changes with cached statements and apply reads them
 * back with cached statements: the values must survive the reuse of a binding
 * slot, and every cached statement must end each call reset on the connection.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "copydb.h"
#include "ld_pgoutput.h"
#include "ld_store.h"
#include "lock_utils.h"

#define NCHANGES 1000
#define NCOLS 4
#define MAXSTMTS 16

static const char *dbfile = "/tmp/pgoutput-store-output.db";
static bool failed = false;

#define CHECK(cond, ...) \
	do { \
		if (!(cond)) { printf("FAIL: " __VA_ARGS__); printf("\n"); failed = true; } \
	} while (0)


/* consecutive binds of a slot see a value then NULL, a value then '' */
static void
make_col(PgoutputColumn *col, int i, int j)
{
	static const char statuses[] = { 't', 'n', 't', 't', 'u', 't' };
	int k = (i + j) % 6;

	sformat(col->name, sizeof(col->name), "c%d", j);
	col->status = statuses[k];
	col->value = NULL;

	if (col->status == 't')
	{
		char buf[64];
		sformat(buf, sizeof(buf), "v%d_%d", i, j);
		col->value = strdup(k == 3 ? "" : buf);
	}
}


static PgoutputColumn *
make_cols(int i, int n)
{
	PgoutputColumn *cols = calloc(n, sizeof(PgoutputColumn));

	for (int j = 0; j < n; j++)
	{
		make_col(&cols[j], i, j);
	}
	return cols;
}


/* I, U, D and T in turn, with every old_type each action can carry */
static void
make_change(PgoutputMessage *msg, int i)
{
	static const char actions[] = { 'I', 'U', 'D', 'T' };
	static const char updateOld[] = { 'K', 'O', 0 };

	memset(msg, 0, sizeof(PgoutputMessage));
	msg->action = actions[i % 4];
	strlcpy(msg->nspname, "public", sizeof(msg->nspname));
	sformat(msg->relname, sizeof(msg->relname), "t%d", i % 7);

	switch (msg->action)
	{
		case 'I':
		{
			msg->ncols_new = NCOLS;
			msg->new_cols = make_cols(i, NCOLS);
			break;
		}

		case 'U':
		{
			msg->oldType = updateOld[(i / 4) % 3];
			if (msg->oldType != 0)
			{
				msg->ncols_old = NCOLS;
				msg->old_cols = make_cols(i + 1, NCOLS);
			}
			msg->ncols_new = NCOLS;
			msg->new_cols = make_cols(i, NCOLS);
			break;
		}

		case 'D':
		{
			msg->oldType = (i / 4) % 2 ? 'O' : 'K';
			msg->ncols_old = NCOLS;
			msg->old_cols = make_cols(i, NCOLS);
			break;
		}

		case 'T':
		{
			msg->oldType = (i / 4) % 2 ? 'R' : 0;
			msg->ncols_new = 1 + i % 3;
			msg->new_cols = calloc(msg->ncols_new, sizeof(PgoutputColumn));
			for (int k = 0; k < msg->ncols_new; k++)
			{
				char rel[64];
				sformat(msg->new_cols[k].name, PG_NAMEDATALEN, "s%d", k);
				sformat(rel, sizeof(rel), "r%d_%d", i, k);
				msg->new_cols[k].status = 't';
				msg->new_cols[k].value = strdup(rel);
			}
			break;
		}
	}
}


static void
make_metadata(LogicalMessageMetadata *metadata, char action, int batch, int i)
{
	memset(metadata, 0, sizeof(LogicalMessageMetadata));
	metadata->action = action;
	metadata->xid = 1000 + batch;
	metadata->lsn = (uint64_t) (batch + 1) * 0x1000000 + i;
	metadata->seq = i;
	strlcpy(metadata->timestamp, "2026-11-23 00:00:00.000000+00",
			sizeof(metadata->timestamp));
}


static int
list_stmts(sqlite3 *db, sqlite3_stmt **stmts)
{
	int n = 0;

	for (sqlite3_stmt *s = sqlite3_next_stmt(db, NULL);
		 s != NULL;
		 s = sqlite3_next_stmt(db, s))
	{
		CHECK(!sqlite3_stmt_busy(s), "statement left mid-step: %s", sqlite3_sql(s));
		if (n < MAXSTMTS)
		{
			stmts[n] = s;
		}
		n++;
	}
	return n;
}


static void
check_same_stmts(sqlite3 *db, sqlite3_stmt **expected, int n, const char *after)
{
	sqlite3_stmt *stmts[MAXSTMTS];
	int count = list_stmts(db, stmts);

	CHECK(count == n, "%d prepared statements after %s, expected %d", count, after, n);

	for (int i = 0; i < n && i < count; i++)
	{
		bool found = false;
		for (int j = 0; j < n; j++)
		{
			found = found || stmts[i] == expected[j];
		}
		CHECK(found, "statement %s was compiled again after %s",
			  sqlite3_sql(stmts[i]), after);
	}
}


static bool
store_batch(DatabaseCatalog *catalog, int batch, PgoutputMessage *changes)
{
	LogicalMessageMetadata metadata;

	make_metadata(&metadata, 'B', batch, 0);
	metadata.jsonBuffer = "{\"action\":\"B\"}";

	if (!ld_store_output_begin(catalog) ||
		!ld_store_insert_message(catalog, &metadata))
	{
		return false;
	}

	for (int i = 0; i < NCHANGES; i++)
	{
		free_pgoutput_message(&changes[i]);
		make_change(&changes[i], i + batch * NCHANGES);
		make_metadata(&metadata, changes[i].action, batch, i + 1);

		if (!ld_store_insert_pgoutput_message(catalog, &metadata, &changes[i]))
		{
			return false;
		}
	}

	return ld_store_output_commit(catalog);
}


/* raw rows: the TEXT binder stores '' as NULL, every other 't' value as given */
static void
check_raw_section(sqlite3 *db, int64_t id, char section,
				  PgoutputColumn *cols, int ncols)
{
	sqlite3_stmt *s = NULL;
	const char *sql =
		"select pos, name, status, value from pgoutput_col "
		" where output_id = ? and section = ? order by pos";
	char sec[2] = { section, '\0' };

	if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK)
	{
		printf("FAIL: %s\n", sqlite3_errmsg(db));
		exit(2);
	}
	sqlite3_bind_int64(s, 1, id);
	sqlite3_bind_text(s, 2, sec, -1, SQLITE_STATIC);

	int n = 0;

	while (sqlite3_step(s) == SQLITE_ROW)
	{
		if (n >= ncols)
		{
			n++;
			continue;
		}

		PgoutputColumn *c = &cols[n];
		const char *name = (const char *) sqlite3_column_text(s, 1);
		const char *status = (const char *) sqlite3_column_text(s, 2);
		bool isNull = sqlite3_column_type(s, 3) == SQLITE_NULL;
		const char *value = (const char *) sqlite3_column_text(s, 3);

		CHECK(sqlite3_column_int(s, 0) == n, "output %lld %c: pos %d at row %d",
			  (long long) id, section, sqlite3_column_int(s, 0), n);
		CHECK(strcmp(name, c->name) == 0, "output %lld %c%d: name %s, expected %s",
			  (long long) id, section, n, name, c->name);
		CHECK(status[0] == c->status && status[1] == '\0',
			  "output %lld %c%d: status %s, expected %c",
			  (long long) id, section, n, status, c->status);
		bool expectNull = c->status != 't' || c->value[0] == '\0';

		CHECK(isNull == expectNull,
			  "output %lld %c%d: status %c input '%s' stored as %s",
			  (long long) id, section, n, c->status,
			  c->value ? c->value : "NULL", isNull ? "NULL" : value);
		CHECK(isNull || strcmp(value, c->value) == 0,
			  "output %lld %c%d: value '%s', expected '%s'",
			  (long long) id, section, n, value, c->value);
		n++;
	}

	CHECK(n == ncols, "output %lld %c: %d rows, expected %d",
		  (long long) id, section, n, ncols);
	sqlite3_finalize(s);
}


static void
check_quoted(const char *got, const char *name, int64_t id)
{
	char expected[PG_NAMEDATALEN + 2];
	sformat(expected, sizeof(expected), "\"%s\"", name);
	CHECK(got != NULL && strcmp(got, expected) == 0,
		  "output %lld: identifier %s, expected %s",
		  (long long) id, got ? got : "NULL", expected);
}


/* the parser drops 'u' columns, and in a key tuple the 'n' placeholders */
static void
check_tuple(LogicalMessageTupleArray *array, PgoutputColumn *cols, int ncols,
			bool key, int64_t id)
{
	CHECK(array->count == 1, "output %lld: %d tuples", (long long) id, array->count);
	if (array->count != 1)
	{
		return;
	}

	LogicalMessageTuple *tuple = &array->array[0];
	int j = 0;

	for (int i = 0; i < ncols; i++)
	{
		PgoutputColumn *c = &cols[i];

		if (c->status == 'u' || (key && c->status == 'n'))
		{
			continue;
		}
		if (j >= tuple->attributes.count)
		{
			j++;
			continue;
		}

		LogicalMessageValue *v = &tuple->values.array[0].array[j];

		check_quoted(tuple->attributes.array[j].attname, c->name, id);
		CHECK(v->isNull == (c->status == 'n'), "output %lld: %s isNull %d",
			  (long long) id, c->name, v->isNull);
		CHECK(v->isNull || strcmp(v->val.str, c->value) == 0,
			  "output %lld: %s value '%s', expected '%s'",
			  (long long) id, c->name, v->val.str, c->value);
		j++;
	}

	CHECK(j == tuple->attributes.count, "output %lld: %d columns, expected %d",
		  (long long) id, tuple->attributes.count, j);
}


static void
check_parsed(DatabaseCatalog *catalog, int64_t id, PgoutputMessage *msg)
{
	StreamContext *context = calloc(1, sizeof(StreamContext));
	LogicalTransactionStatement *stmt = calloc(1, sizeof(LogicalTransactionStatement));

	context->stmt = stmt;
	context->metadata.action = msg->action;

	if (!parsePgoutputMessage(context, catalog, id))
	{
		CHECK(false, "output %lld: parsePgoutputMessage failed", (long long) id);
		return;
	}

	bool keyOnly = msg->oldType != 'O';
	LogicalMessageRelation *table =
		msg->action == 'I' ? &stmt->stmt.insert.table :
		msg->action == 'U' ? &stmt->stmt.update.table : &stmt->stmt.delete.table;

	if (msg->action != 'T')
	{
		check_quoted(table->nspname, msg->nspname, id);
		check_quoted(table->relname, msg->relname, id);
	}

	switch (msg->action)
	{
		case 'I':
		{
			check_tuple(&stmt->stmt.insert.new, msg->new_cols, msg->ncols_new,
						false, id);
			break;
		}

		case 'U':
		{
			check_tuple(&stmt->stmt.update.old, msg->old_cols, msg->ncols_old,
						keyOnly, id);
			check_tuple(&stmt->stmt.update.new, msg->new_cols, msg->ncols_new,
						false, id);
			break;
		}

		case 'D':
		{
			check_tuple(&stmt->stmt.delete.old, msg->old_cols, msg->ncols_old,
						keyOnly, id);
			break;
		}

		case 'T':
		{
			LogicalMessageTruncate *t = &stmt->stmt.truncate;

			CHECK(t->count == msg->ncols_new, "output %lld: %d truncated tables",
				  (long long) id, t->count);
			for (int k = 0; k < t->count && k < msg->ncols_new; k++)
			{
				check_quoted(t->tables[k].nspname, msg->new_cols[k].name, id);
				check_quoted(t->tables[k].relname, msg->new_cols[k].value, id);
			}
			CHECK(t->restartIdentity == (msg->oldType == 'R'),
				  "output %lld: restartIdentity %d", (long long) id,
				  t->restartIdentity);
			break;
		}
	}
}


static void
check_batch(DatabaseCatalog *catalog, int batch, PgoutputMessage *changes)
{
	for (int i = 0; i < NCHANGES; i++)
	{
		PgoutputMessage *msg = &changes[i];
		LogicalMessageMetadata metadata;
		sqlite3_stmt *s = NULL;
		int64_t id = -1;

		make_metadata(&metadata, msg->action, batch, i + 1);

		if (sqlite3_prepare_v2(catalog->db,
							   "select id from output where lsn = ? and seq = ?",
							   -1, &s, NULL) != SQLITE_OK)
		{
			printf("FAIL: %s\n", sqlite3_errmsg(catalog->db));
			exit(2);
		}
		sqlite3_bind_int64(s, 1, (sqlite3_int64) metadata.lsn);
		sqlite3_bind_int64(s, 2, metadata.seq);
		if (sqlite3_step(s) == SQLITE_ROW)
		{
			id = sqlite3_column_int64(s, 0);
		}
		sqlite3_finalize(s);

		CHECK(id > 0, "change %d of batch %d has no output row", i, batch);
		if (id <= 0)
		{
			continue;
		}

		char oldSection = msg->oldType == 'O' ? 'O' : 'K';
		char newSection = msg->action == 'T' ? 'T' : 'N';

		check_raw_section(catalog->db, id, oldSection,
						  msg->old_cols, msg->old_cols ? msg->ncols_old : 0);
		check_raw_section(catalog->db, id, newSection,
						  msg->new_cols, msg->new_cols ? msg->ncols_new : 0);
		check_parsed(catalog, id, msg);
	}
}


int
main(void)
{
	DatabaseCatalog *catalog = calloc(1, sizeof(DatabaseCatalog));
	PgoutputMessage *changes = calloc(NCHANGES, sizeof(PgoutputMessage));
	sqlite3_stmt *stmts[MAXSTMTS];

	setvbuf(stdout, NULL, _IOLBF, 0);

	/* the SQLite level logs every bind, thousands of lines per batch */
	log_set_level(LOG_INFO);

	(void) unlink(dbfile);
	catalog->type = DATABASE_CATALOG_TYPE_OUTPUT;
	strlcpy(catalog->dbfile, dbfile, sizeof(catalog->dbfile));

	if (!catalog_init(catalog))
	{
		printf("FAIL: could not create %s\n", dbfile);
		return 2;
	}

	CHECK(list_stmts(catalog->db, stmts) == 0, "statements prepared by catalog_init");

	/* the BEGIN row, the delete, the output row and the column row */
	if (!store_batch(catalog, 0, changes))
	{
		printf("FAIL: could not store batch 0\n");
		return 2;
	}
	int n = list_stmts(catalog->db, stmts);
	CHECK(n == 4, "%d prepared statements after storing, expected 4", n);
	check_batch(catalog, 0, changes);

	/* and the two pgoutput_col and output readers */
	n = list_stmts(catalog->db, stmts);
	CHECK(n == 6, "%d prepared statements after reading, expected 6", n);
	printf("ok: batch 0 stored and read back with %d cached statements\n", n);

	if (n > MAXSTMTS || !store_batch(catalog, 1, changes))
	{
		printf("FAIL: could not store batch 1\n");
		return 2;
	}
	check_same_stmts(catalog->db, stmts, n, "storing batch 1");
	check_batch(catalog, 1, changes);
	check_same_stmts(catalog->db, stmts, n, "reading batch 1");
	printf("ok: batch 1 reused the same %d statements\n", n);

	CHECK(catalog_close(catalog), "catalog_close failed");
	(void) semaphore_finish(&catalog->sema);

	if (failed)
	{
		printf("FAIL\n");
		return 1;
	}

	printf("PASS: pgoutput changes survive cached receive and reader statements\n");
	return 0;
}
