--
-- This script demonstrates the use of an intersection
-- table to describe the many-to-many relationship
-- between music collections and tracks.
--
select a.COLN_NAME||' '||a.coln_versn AS COLN_NAME_VERSN,
c.track_name,c.track_len 
from MUSIC_COLLECTIONS A,MUSIC_COLLECTION_TRACKS B,
MUSIC_TRACKS C
WHERE A.COLN_ID=B.COLN_ID 
AND B.TRACK_ID=C.TRACK_ID
AND A.COLN_ID < 3
ORDER BY A.COLN_ID,B.TRACK_SEQ;
