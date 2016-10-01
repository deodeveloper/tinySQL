select * from music_artists;
update music_artists set artst_name='Boy Meets World' where artst_name='Jimmy Eat World';
select * from music_artists;
delete from music_artists where artst_name like '%Meets%';
select * from music_artists;
