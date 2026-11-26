---
--- pgcopydb tests/cdc-partitioned-target/dml-only.sql
---
--- DML phase 2: UPDATE and DELETE on an inheritance parent, which must not
--- reach its child on the target, and keyed and keyless UPDATE and DELETE on
--- tables that are partitioned on the target, which must reach their rows.
---

update only partitioned_target.ip set v = 'changed' where id = 1;
delete from only partitioned_target.ip where id = 2;

update partitioned_target.events set payload = 'A' where id = 1;
delete from partitioned_target.events where id = 3;

update partitioned_target."k ONLY k" set "x FROM ONLY y" = 'A' where id = 1;
delete from partitioned_target."k ONLY k" where id = 2;

update partitioned_target."n ONLY n" set "x FROM ONLY y" = 'A'
 where "x FROM ONLY y" = 'a';
delete from partitioned_target."n ONLY n" where "x FROM ONLY y" = 'b';
