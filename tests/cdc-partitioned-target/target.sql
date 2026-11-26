---
--- pgcopydb tests/cdc-partitioned-target/target.sql
---
--- Target: partitioned (relkind='p') table with the same qualified name as
--- the source flat table. Postgres rejects "TRUNCATE ONLY" against a
--- partitioned table; the apply path must rewrite the replicated
--- statement to plain "TRUNCATE".
---

create schema partitioned_target;

create table partitioned_target.events (
    id bigint not null,
    bucket smallint not null,
    payload text,
    primary key (id, bucket)
) partition by list (bucket);

create table partitioned_target.events_a
    partition of partitioned_target.events for values in (0);

create table partitioned_target.events_b
    partition of partitioned_target.events for values in (1);

create table partitioned_target.keyless (
    bucket smallint not null,
    payload text
) partition by list (bucket);

create table partitioned_target.keyless_a
    partition of partitioned_target.keyless for values in (0);

create table partitioned_target.keyless_b
    partition of partitioned_target.keyless for values in (1);

create table partitioned_target.ip (id integer primary key, v text);
create table partitioned_target.ic () inherits (partitioned_target.ip);
alter table partitioned_target.ic add primary key (id);

create table partitioned_target."k ONLY k" (
    id bigint not null,
    bucket smallint not null,
    "x FROM ONLY y" text,
    primary key (id, bucket)
) partition by list (bucket);

create table partitioned_target."k ONLY k_a"
    partition of partitioned_target."k ONLY k" for values in (0);

create table partitioned_target."k ONLY k_b"
    partition of partitioned_target."k ONLY k" for values in (1);

create table partitioned_target."n ONLY n" (
    bucket smallint not null,
    "x FROM ONLY y" text
) partition by list (bucket);

create table partitioned_target."n ONLY n_a"
    partition of partitioned_target."n ONLY n" for values in (0);

create table partitioned_target."n ONLY n_b"
    partition of partitioned_target."n ONLY n" for values in (1);
