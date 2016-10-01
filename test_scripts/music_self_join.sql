--
-- This script demonstrates the ability of tinySQL to do self-joins
-- It is assumed that you are connected to the music database directory
--
select a.artst_name,b.artst_name from music_artists a,music_artists b where (UPPER(a.artst_name) like 'THE%' and UPPER(b.artst_name) like 'THE%') and a.artst_name > b.artst_name;
