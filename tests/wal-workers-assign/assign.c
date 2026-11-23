/*
 * wal_workers_assign must place every table deterministically and refuse the
 * lists it cannot honour; the catalog must store and return the assignment.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "catalog.h"
#include "ld_stream.h"

#define MAX_TABLES 64

static bool failed = false;


static void
check(bool ok, const char *what)
{
	printf("%s: %s\n", ok ? "ok" : "FAIL", what);
	failed = failed || !ok;
}


static WalWorkerTable
T(uint32_t oid, const char *relname, uint32_t family, int64_t weight,
  int64_t bytes)
{
	WalWorkerTable table = {
		.oid = oid,
		.family = family == 0 ? oid : family,
		.weight = weight,
		.bytes = bytes,
		.worker = -1
	};

	strlcpy(table.nspname, "s", sizeof(table.nspname));
	strlcpy(table.relname, relname, sizeof(table.relname));

	return table;
}


/* expected[i] is the worker of tables[i] */
static void
expect(const char *what, WalWorkerTable *tables, int count, int workers,
	   const char *highTraffic, const int *expected)
{
	bool ok = wal_workers_assign(tables, count, workers, highTraffic);

	for (int i = 0; ok && i < count; i++)
	{
		if (tables[i].worker != expected[i])
		{
			printf("  %s.%s: worker %d, expected %d\n",
				   tables[i].nspname, tables[i].relname,
				   tables[i].worker, expected[i]);
			ok = false;
		}
	}

	check(ok, what);
}


static void
refuse(const char *what, WalWorkerTable *tables, int count, int workers,
	   const char *highTraffic)
{
	check(!wal_workers_assign(tables, count, workers, highTraffic), what);
}


static void
test_assign(void)
{
	/* c beats b on bytes, d beats e on family oid, d goes to fewer bytes */
	WalWorkerTable ties[] = {
		T(10, "a", 0, 100, 0), T(11, "b", 0, 50, 5), T(12, "c", 0, 50, 10),
		T(13, "d", 0, 30, 1), T(14, "e", 0, 30, 1)
	};
	expect("heaviest first, ties by bytes, count and family oid",
		   ties, 5, 3, "", (int[]) { 0, 2, 1, 2, 1 });

	WalWorkerTable fewer[] = {
		T(20, "hot", 0, 1, 0), T(21, "a", 0, 100, 0),
		T(22, "b", 0, 50, 0), T(23, "c", 0, 40, 0)
	};
	expect("H < N keeps worker 0 for its high-traffic table",
		   fewer, 4, 3, "s.hot", (int[]) { 0, 1, 2, 2 });

	/* x evens out worker 0, then y goes to the worker with fewer units */
	WalWorkerTable equal[] = {
		T(30, "h0", 0, 3, 0), T(31, "h1", 0, 5, 0),
		T(32, "x", 0, 2, 0), T(33, "y", 0, 0, 0)
	};
	expect("H == N spreads the others over all workers, ties by unit count",
		   equal, 4, 2, "s.h0,s.h1", (int[]) { 0, 1, 0, 1 });

	WalWorkerTable family[] = {
		T(40, "root", 0, 0, 0), T(41, "p_1", 40, 30, 0),
		T(42, "p_2", 40, 30, 0), T(50, "t", 0, 50, 0)
	};
	expect("a partition family is one unit",
		   family, 4, 2, "", (int[]) { 0, 0, 0, 1 });
	expect("a high-traffic partition takes its family",
		   family, 4, 2, "s.p_2", (int[]) { 0, 0, 0, 1 });
	expect("the high-traffic list order sets the workers",
		   family, 4, 2, "s.t,s.root", (int[]) { 1, 1, 1, 0 });

	refuse("an entry matching no table", family, 4, 2, "s.missing");
	refuse("an entry differing in case", family, 4, 2, "S.t");
	refuse("an entry without a schema", family, 4, 2, "t");
	refuse("an empty entry", family, 4, 2, "s.t,");
	refuse("two entries in one family", family, 4, 2, "s.p_1,s.p_2");
	refuse("one entry twice", family, 4, 3, "s.t,s.t");
	refuse("more entries than workers", ties, 5, 2, "s.a,s.b,s.c");
	refuse("a worker without a table", family, 4, 3, "");
	refuse("no table at all", family, 0, 1, "");

	WalWorkerTable twins[] = { T(60, "t", 0, 1, 0), T(61, "t", 0, 1, 0) };
	refuse("an entry matching two tables", twins, 2, 2, "s.t");
}


static void
test_shuffle(void)
{
	WalWorkerTable tables[MAX_TABLES];
	WalWorkerTable shuffled[MAX_TABLES];
	int count = 40;
	uint32_t seed = 42;

	/* few distinct weights and bytes, so that ties are common */
	for (int i = 0; i < count; i++)
	{
		char name[NAMEDATALEN];
		uint32_t oid = 1000 + i;
		uint32_t family = i % 5 == 1 ? oid - 1 : oid;

		sformat(name, sizeof(name), "t%d", i);
		tables[i] = T(oid, name, family, (i * 7) % 4 * 10, (i * 3) % 2);
	}

	bool ok = wal_workers_assign(tables, count, 6, "s.t7,s.t20");

	for (int round = 0; ok && round < 20; round++)
	{
		for (int i = 0; i < count; i++)
		{
			shuffled[i] = tables[i];
			shuffled[i].worker = -1;
		}

		for (int i = count - 1; i > 0; i--)
		{
			seed = seed * 1103515245 + 12345;

			int j = (int) ((seed >> 16) % (uint32_t) (i + 1));
			WalWorkerTable swap = shuffled[i];

			shuffled[i] = shuffled[j];
			shuffled[j] = swap;
		}

		ok = wal_workers_assign(shuffled, count, 6, "s.t7,s.t20");

		for (int i = 0; ok && i < count; i++)
		{
			ok = shuffled[i].worker == tables[shuffled[i].oid - 1000].worker;
		}
	}

	check(ok, "shuffled input gives the same assignment");
}


typedef struct Rows
{
	WalWorkerTable *expected;
	int count;
	bool ok;
} Rows;


static bool
compare_row(void *context, WalWorkerTable *table)
{
	Rows *rows = (Rows *) context;
	WalWorkerTable *e = &(rows->expected[rows->count++]);

	rows->ok = rows->ok &&
			   table->oid == e->oid && table->family == e->family &&
			   table->weight == e->weight && table->bytes == e->bytes &&
			   table->worker == e->worker &&
			   strcmp(table->nspname, e->nspname) == 0 &&
			   strcmp(table->relname, e->relname) == 0;

	return true;
}


static void
test_catalog(void)
{
	char dir[] = "/tmp/wal-workers-assign.XXXXXX";
	DatabaseCatalog catalog = { .type = DATABASE_CATALOG_TYPE_SOURCE };

	if (mkdtemp(dir) == NULL)
	{
		printf("FAIL: mkdtemp\n");
		exit(2);
	}

	sformat(catalog.dbfile, sizeof(catalog.dbfile), "%s/source.db", dir);

	if (!catalog_init(&catalog))
	{
		printf("FAIL: catalog_init\n");
		exit(2);
	}

	int workers = 0;
	char list[64] = { 0 };
	bool found = true;

	check(catalog_read_wal_worker_setup(&catalog, &workers, list,
										sizeof(list), &found) && !found,
		  "a new catalog has no assignment");

	/* rows are read back by oid */
	WalWorkerTable tables[] = {
		T(12, "c", 0, 7, 70), T(11, "b", 0, 9, 90), T(13, "d", 11, 0, 1)
	};
	tables[0].worker = 1;
	tables[1].worker = 0;
	tables[2].worker = 1;

	check(catalog_write_wal_workers(&catalog, 2, "s.b", tables, 3),
		  "write the assignment");
	check(catalog_read_wal_worker_setup(&catalog, &workers, list,
										sizeof(list), &found) &&
		  found && workers == 2 && strcmp(list, "s.b") == 0,
		  "read the worker count and list back");
	check(!catalog_read_wal_worker_setup(&catalog, &workers, list, 3, &found),
		  "a list longer than the buffer fails");

	Rows rows = {
		.expected = (WalWorkerTable[]) { tables[1], tables[0], tables[2] },
		.ok = true
	};
	check(catalog_iter_wal_worker_table(&catalog, &rows, &compare_row) &&
		  rows.ok && rows.count == 3,
		  "read the table rows back by oid");

	char pubs[64] = { 0 };

	check(catalog_wal_worker_publications(&catalog, "slot", 1,
										  pubs, sizeof(pubs)) &&
		  strcmp(pubs, "slot_t12,slot_t13") == 0,
		  "list the publications of one worker by oid");
	check(!catalog_wal_worker_publications(&catalog, "slot", 1, pubs, 17) &&
		  pubs[0] == '\0',
		  "a publication list longer than the buffer fails");
	check(!catalog_wal_worker_publications(&catalog, "slot", 2,
										   pubs, sizeof(pubs)),
		  "a worker without a table fails");

	check(catalog_write_wal_workers(&catalog, 1, "", tables, 1) &&
		  catalog_read_wal_worker_setup(&catalog, &workers, list,
										sizeof(list), &found) &&
		  workers == 1 && list[0] == '\0',
		  "a second write replaces the first");

	rows = (Rows) {
		.expected = tables, .ok = true
	};
	check(catalog_iter_wal_worker_table(&catalog, &rows, &compare_row) &&
		  rows.ok && rows.count == 1,
		  "a second write replaces the table rows");

	/* a catalog of an older pgcopydb lacks both tables */
	check(sqlite3_exec(catalog.db,
					   "drop table wal_worker_setup; drop table wal_worker_table",
					   NULL, NULL, NULL) == SQLITE_OK &&
		  catalog_close(&catalog) && catalog_init(&catalog) &&
		  catalog_read_wal_worker_setup(&catalog, &workers, list,
										sizeof(list), &found) && !found,
		  "opening an older catalog adds the tables");

	(void) catalog_close(&catalog);
	(void) semaphore_finish(&(catalog.sema));
	(void) unlink(catalog.dbfile);
	(void) rmdir(dir);
}


int
main(void)
{
	setvbuf(stdout, NULL, _IOLBF, 0);
	log_set_level(LOG_WARN);

	test_assign();
	test_shuffle();
	test_catalog();

	if (failed)
	{
		printf("FAIL\n");
		return 1;
	}

	printf("PASS: WAL worker assignment and catalog round trip\n");
	return 0;
}
