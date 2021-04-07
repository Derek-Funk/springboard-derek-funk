SET @date_param_begin = '2010-01-01 00:00:00';
SET @date_param_end = '2020-12-20 00:00:00';

-- msd_track
INSERT IGNORE INTO msd_1.msd_track
SELECT * FROM msd_0.msd_track WHERE upload_timestamp > @date_param_begin AND upload_timestamp < @date_param_end;

-- msd_segment
INSERT IGNORE INTO msd_1.msd_segment
SELECT * FROM msd_0.msd_segment WHERE track_id IN (
	SELECT track_id FROM msd_1.msd_track
);

-- msd_section
INSERT IGNORE INTO msd_1.msd_section
SELECT * FROM msd_0.msd_section WHERE track_id IN (
	SELECT track_id FROM msd_1.msd_track
);

-- msd_bar
INSERT IGNORE INTO msd_1.msd_bar
SELECT * FROM msd_0.msd_bar WHERE track_id IN (
	SELECT track_id FROM msd_1.msd_track
);

-- msd_beat
INSERT IGNORE INTO msd_1.msd_beat
SELECT * FROM msd_0.msd_beat WHERE track_id IN (
	SELECT track_id FROM msd_1.msd_track
);

-- msd_tatum
INSERT IGNORE INTO msd_1.msd_tatum
SELECT * FROM msd_0.msd_tatum WHERE track_id IN (
	SELECT track_id FROM msd_1.msd_track
);

-- msd_artist
INSERT IGNORE INTO msd_1.msd_artist
SELECT * FROM msd_0.msd_artist WHERE artist_id IN (
	SELECT artist_id FROM msd_1.msd_track
);

-- msd_artist_similarity
INSERT IGNORE INTO msd_1.msd_artist_similarity
SELECT * FROM msd_0.msd_artist_similarity WHERE
	artist_id_1 IN (SELECT artist_id FROM msd_1.msd_artist) OR
    artist_id_2 IN (SELECT artist_id FROM msd_1.msd_artist);
    
-- msd_artist_term
INSERT IGNORE INTO msd_1.msd_artist_term
SELECT * FROM msd_0.msd_artist_term WHERE artist_id IN (
	SELECT artist_id FROM msd_1.msd_artist
);

-- msd_artist_mbtag
INSERT IGNORE INTO msd_1.msd_artist_mbtag
SELECT * FROM msd_0.msd_artist_mbtag WHERE artist_id IN (
	SELECT artist_id FROM msd_1.msd_artist
);

-- msd_r_term
INSERT IGNORE INTO msd_1.msd_r_term
SELECT * FROM msd_0.msd_r_term;

-- msd_r_mbtag
INSERT IGNORE INTO msd_1.msd_r_mbtag
SELECT * FROM msd_0.msd_r_mbtag;