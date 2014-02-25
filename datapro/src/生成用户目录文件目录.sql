insert into fileset select distinct filename,0 FROM test.teif_1;
insert into fileset  select distinct filename,0 FROM test.teif_2 where filename not in (select fileset.filename from fileset);
insert into fileset  select distinct filename,0 FROM test.teif_3 where filename not in (select fileset.filename from fileset);
insert into userset select distinct email,0 FROM test.teif_1;
insert into userset  select distinct email,0 FROM test.teif_2 where email not in (select userset.email from userset);
insert into userset  select distinct email,0 FROM test.teif_3 where email not in (select userset.email from userset);