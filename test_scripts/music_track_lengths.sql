--
-- This script demonstrates the CONCAT,MAX,MIN, and SUM functions as
-- well as the ability to mix columns with string constants
-- It is assumed that you are connected to the MUSIC database directory
--
select CONCAT('Track lengths are ',min(track_len),' to ',max(track_len),'.  Sum of track lengths is ',SUM(track_len)) as 'Track Length Summary' from music_tracks;
