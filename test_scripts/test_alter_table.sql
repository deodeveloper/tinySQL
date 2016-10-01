--
-- This script demonstrates the ability of tinySQL to use long column names
--
set echo on;
describe longtest;
alter table longtest add longcolumnname8 char(8);
alter table longtest add newcol char(20);
describe longtest;
alter table longtest rename newcol to newlongcolumnname;
describe longtest;
exit;
