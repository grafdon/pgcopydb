\set id random(1, 1000)
\set a random(0, 49)
\set op random(1, 100)
begin;
insert into m_log (op) values (:op);
\if :op <= 25
update m1 set v = v + 1, note = case when v % 2 = 0 then null else 'u' end where id = :id;
\elif :op <= 40
delete from m1 where id = :id;
\elif :op <= 60
insert into m1 values (:id, 0, null) on conflict (id) do update set v = m1.v + 100;
\elif :op <= 68
update m1 set id = -nextval('m_seq') where id = :id;
\elif :op <= 80
insert into m2 values (:a, null), (:a, null);
\elif :op <= 90
update m2 set b = 'u' || :op where a = :a;
\elif :op <= 99
delete from m2 where a = :a and b is distinct from 'b';
\else
truncate m2;
\endif
end;
