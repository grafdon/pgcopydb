-- Workload M: small tables that every kind of change hits, to check the reader
-- and the dependency waits on DELETE, key changes, NULL, duplicate rows and TRUNCATE.

\set ON_ERROR_STOP on

create table m1 (id bigint primary key, v int not null, note text);
insert into m1 select i, 0, case when i % 3 = 0 then null else 'n' || i end from generate_series(1, 1000) i;
create sequence m_seq;

-- keyless with NULLs and identical rows
create table m2 (a int, b text);
alter table m2 replica identity full;
insert into m2 select i % 50, case when i % 4 = 0 then null else 'b' end from generate_series(1, 200) i;

-- one insert per transaction, so pgoutput sends every transaction
create table m_log (op int not null, at timestamptz not null default clock_timestamp());
