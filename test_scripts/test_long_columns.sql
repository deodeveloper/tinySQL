--
-- This script demonstrates the ability of tinySQL to use long column names
--
set echo on;
drop table longtest;
create table longtest (shortcol char(12),longcolumnname7 char(27));
insert into longtest (longcolumnname7) values ('start with nothing');
select * from longtest;
describe longtest;
update longtest set longcolumnname7='Davis' where TRIM(longcolumnname7)='start with nothing';
select * from longtest;
exit;
