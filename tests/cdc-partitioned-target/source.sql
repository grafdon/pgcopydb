---
--- pgcopydb tests/cdc-partitioned-target/source.sql
---
--- Source: flat (relkind='r') table. Pairs with target.sql to build the
--- asymmetric source-flat / target-partitioned case that the apply-side
--- TRUNCATE rewrite has to handle.
---

create schema partitioned_target;

create table partitioned_target.events (
    id bigint primary key,
    bucket smallint not null,
    payload text
);

-- Seed rows that exist before the snapshot, copied to the target by
-- pgcopydb copy table-data and then wiped by the replicated TRUNCATE.
insert into partitioned_target.events (id, bucket, payload)
     values (1, 0, 'a'),
            (2, 1, 'b'),
            (3, 0, 'c');

-- Keyless REPLICA IDENTITY FULL table: a replayed change finds its row by
-- (tableoid, ctid), because ctid alone repeats across the target partitions.
create table partitioned_target.keyless (
    bucket smallint not null,
    payload text
);

alter table partitioned_target.keyless replica identity full;

insert into partitioned_target.keyless (bucket, payload)
     values (0, 'a'),
            (1, 'b');

-- Inheritance pair with the same keys in parent and child: an UPDATE or
-- DELETE of the parent row must not reach the child row on the target.
create table partitioned_target.ip (id integer primary key, v text);
create table partitioned_target.ic () inherits (partitioned_target.ip);
alter table partitioned_target.ic add primary key (id);

insert into partitioned_target.ip (id, v) values (1, 'p1'), (2, 'p2');
insert into partitioned_target.ic (id, v) values (1, 'c1'), (2, 'c2');

-- Names that hold the word ONLY, partitioned on the target: apply must find
-- the ONLY that the statement builder wrote, not a word inside an identifier.
create table partitioned_target."k ONLY k" (
    id bigint primary key,
    bucket smallint not null,
    "x FROM ONLY y" text
);

insert into partitioned_target."k ONLY k" (id, bucket, "x FROM ONLY y")
     values (1, 0, 'a'),
            (2, 1, 'b');

create table partitioned_target."n ONLY n" (
    bucket smallint not null,
    "x FROM ONLY y" text
);

alter table partitioned_target."n ONLY n" replica identity full;

insert into partitioned_target."n ONLY n" (bucket, "x FROM ONLY y")
     values (0, 'a'),
            (1, 'b');
