--
-- This script demonstrates the ability to drop and create tables.
-- It is assumed that you are connected to the MUSIC database directory.
--
drop table music_artists;
create table music_artists (ARTST_ID INT,ARTST_NAME CHAR(60));
insert into music_artists(ARTST_ID,ARTST_NAME) VALUES (1,'The Beatles');
insert into music_artists(ARTST_ID,ARTST_NAME) VALUES (2,'The Rolling Stones');
insert into music_artists(ARTST_ID,ARTST_NAME) VALUES (3,'Red Hot Chili Peppers');
insert into music_artists(ARTST_ID,ARTST_NAME) VALUES (4,'Jimmy Eat World');
select * from music_artists;
