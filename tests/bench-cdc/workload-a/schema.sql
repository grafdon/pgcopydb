-- 32 units of 100k seed rows; see README.md for how units are counted.
-- Every row carries ts = clock_timestamp() of its last write, for end-to-end lag.

\set ON_ERROR_STOP on

do $$
declare
  t text;
begin
  -- hot tables and warm/cold tables share one shape
  foreach t in array array['h1','h2','h3','h4',
    'w01','w02','w03','w04','w05','w06','w07','w08',
    'w09','w10','w11','w12','w13','w14','w15','w16','idle']
  loop
    execute format(
      'create table %I (id bigserial primary key, k int not null, v int not null,
         payload text not null, ts timestamptz not null default clock_timestamp())', t);
    execute format(
      'insert into %I (k, v, payload)
         select i %% 1000, 0, repeat(md5(i::text), 3) from generate_series(1, 100000) i', t);
  end loop;

  for i in 1..4 loop
    execute format(
      'create table fk%s_p (id bigserial primary key, k int not null, v int not null,
         payload text not null, ts timestamptz not null default clock_timestamp())', i);
    execute format(
      'create table fk%s_c (id bigserial primary key,
         parent_id bigint not null references fk%s_p (id), v int not null,
         payload text not null, ts timestamptz not null default clock_timestamp())', i, i);
    execute format(
      'insert into fk%s_p (k, v, payload)
         select i %% 1000, 0, repeat(md5(i::text), 3) from generate_series(1, 100000) i', i);
    execute format(
      'insert into fk%s_c (parent_id, v, payload)
         select i, 0, repeat(md5(i::text), 3) from generate_series(1, 100000) i', i);
  end loop;

  -- keyless: no primary key or unique index; the plain index on id keeps the
  -- full-identity UPDATE on the target from scanning the whole table
  foreach t in array array['kl1','kl2'] loop
    execute format(
      'create table %I (id bigint not null, k int not null, v int not null,
         payload text not null, ts timestamptz not null default clock_timestamp())', t);
    execute format('create index on %I (id)', t);
    execute format('alter table %I replica identity full', t);
    execute format(
      'insert into %I (id, k, v, payload)
         select i, i %% 1000, 0, repeat(md5(i::text), 3) from generate_series(1, 100000) i', t);
  end loop;
end
$$;

create table part (id bigserial primary key, k int not null, v int not null,
  payload text not null, ts timestamptz not null default clock_timestamp())
  partition by hash (id);
create table part_0 partition of part for values with (modulus 4, remainder 0);
create table part_1 partition of part for values with (modulus 4, remainder 1);
create table part_2 partition of part for values with (modulus 4, remainder 2);
create table part_3 partition of part for values with (modulus 4, remainder 3);
insert into part (k, v, payload)
  select i % 1000, 0, repeat(md5(i::text), 3) from generate_series(1, 100000) i;

vacuum analyze;
checkpoint;
