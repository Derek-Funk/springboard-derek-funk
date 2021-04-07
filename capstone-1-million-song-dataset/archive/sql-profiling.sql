use msd;

SET profiling = 0;
SET profiling = 1;
select * from msd_artist;
SHOW PROFILES;
select * from msd_segment;

select * from information_schema.profiling;

EXPLAIN
SELECT t.title
FROM msd_track AS t
INNER JOIN msd_artist AS a USING(artist_id)
WHERE a.artist_name = 'Britney Spears';

EXPLAIN
SELECT title
FROM msd_track
WHERE artist_id = (
	SELECT artist_id
	FROM msd_artist
	WHERE artist_name = 'Britney Spears'
);

-- AR03BDP1187FB5B324
SELECT artist_id
	FROM msd_artist
	WHERE artist_name = 'Britney Spears';
    
EXPLAIN
SELECT title
FROM msd_track
WHERE artist_id = 'AR03BDP1187FB5B324';