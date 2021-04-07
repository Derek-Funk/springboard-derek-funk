use msd_1;
delete from msd_artist;
delete from msd_artist_similarity;
delete from msd_artist_term;
delete from msd_artist_mbtag;
delete from msd_track;
delete from msd_bar;
delete from msd_beat;
delete from msd_section;
delete from msd_tatum;
delete from msd_segment;

SELECT COUNT(*) from msd_artist;
SELECT COUNT(*) from msd_artist_similarity;
SELECT COUNT(*) from msd_artist_term;
SELECT COUNT(*) from msd_artist_mbtag;
SELECT COUNT(*) from msd_track;
SELECT COUNT(*) from msd_bar;
SELECT COUNT(*) from msd_beat;
SELECT COUNT(*) from msd_section;
SELECT COUNT(*) from msd_tatum;
SELECT COUNT(*) from msd_segment;

SHOW PROCESSLIST;