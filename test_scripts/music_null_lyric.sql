--
-- This scripts tests the use of NULL and NOT NULL in where clauses
--
select * from music_lyrics where lyric like '%bird%';
select 'Count Null lyrics: ',count(*) from music_lyrics where lyric is null;
select 'Count Not Null lyrics: ',count(*) from music_lyrics where lyric is not null;

