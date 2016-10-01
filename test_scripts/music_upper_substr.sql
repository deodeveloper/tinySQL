--
-- This scripts tests the UPPER and SUBSTR functions.
--
select upper(substr(max(artst_name),2,8)) from music_artists;
