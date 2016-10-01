--
-- This script demonstrates support for summary functions
--
set echo on;
SELECT 'Artst ID value range: '||MIN(ARTST_ID)||' to '||MAX(ARTST_ID) FROM MUSIC_ARTISTS;
SELECT 'Artists Count: '||COUNT(*) FROM MUSIC_ARTISTS;
SELECT ARTST_NAME FROM MUSIC_ARTISTS WHERE ARTST_NAME LIKE 'The%';
SELECT 'Count with The: '||COUNT(*) FROM MUSIC_ARTISTS WHERE ARTST_NAME LIKE 'The%';
SELECT 'Count ARTST_ID  > 999: '||COUNT(*) FROM MUSIC_ARTISTS WHERE ARTST_ID > 999;
SELECT MIN(ARTST_NAME),MAX(ARTST_NAME) FROM MUSIC_ARTISTS WHERE ARTST_ID>1;
SELECT 'Count Ziggy: '||COUNT(*) FROM MUSIC_ARTISTS WHERE ARTST_NAME='Ziggy Stardust';
SELECT 'Event Date Range: ',min(event_date),max(event_date),to_date('15-sep-6') from music_events;
exit;
