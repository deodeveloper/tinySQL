--
-- This script demonstrates and/or and like in where clauses
--
select * from music_tracks where track_id > 100 and (track_name like '% together %' or track_name like '%sun%');
