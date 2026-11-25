-- Workload B: 8 tables of 20k seed rows, each row about 1.6 kB of base64
-- text from sha512, which neither pglz nor lz4 shrinks. That stays below the
-- 2 kB TOAST threshold, so every change carries its whole row inline.

\set ON_ERROR_STOP on

create function rnd_payload(seed bigint) returns text
  language sql immutable parallel safe as $f$
    select translate(encode(string_agg(sha512((seed || '.' || g)::bytea), ''::bytea order by g), 'base64'), E'\n', '')
      from generate_series(1, 18) g
  $f$;

do $$
begin
  for i in 1..8 loop
    execute format(
      'create table b%s (id bigserial primary key, k int not null, v int not null,
         payload text not null, ts timestamptz not null default clock_timestamp())', i);
    execute format(
      'insert into b%s (k, v, payload)
         select g %% 1000, 0, rnd_payload(%s * 1000000 + g) from generate_series(1, 20000) g', i, i);
  end loop;
end
$$;

vacuum analyze;
checkpoint;
