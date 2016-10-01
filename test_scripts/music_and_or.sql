--
-- This script demonstrates support for and/or combinations in where clauses
-- as well as the selection of null column values (COLN_VERSN)
-- It is assumed that you are connected to the MUSIC database directory
--
SELECT C.COLN_NAME,C.COLN_VERSN,A.TRACK_NAME FROM MUSIC_TRACKS A,
MUSIC_COLLECTION_TRACKS B,MUSIC_COLLECTIONS C
WHERE A.TRACK_ID=B.TRACK_ID AND B.COLN_ID=C.COLN_ID AND
(A.TRACK_NAME='Get Back' OR A.TRACK_NAME='Rocky Racoon' 
OR A.TRACK_NAME='Help!' OR A.TRACK_NAME='Norwegian Wood');
exit;
