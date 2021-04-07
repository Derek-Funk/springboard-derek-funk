import datetime
import sqlalchemy as db
import sqlalchemy_utils as db_utils

##### DATABASE STEP
# DB connections set up
server = 'localhost'
database = 'songDB'
port = 3306
username = 'root'
password = 'Leanmeanandg!1'
server_conn_string = f'mysql+mysqldb://{username}:{password}@{server}:{port}'

# 1st connection is to msd_0
db_name_0 = 'msd_0'
db_conn_string_0 = server_conn_string + '/' + db_name_0 + '?charset=utf8mb4'
engine_0 = db.create_engine(db_conn_string_0)
connection_0 = engine_0.connect()

# 2nd connection is to msd_1
db_name_1 = 'msd_1'
db_conn_string_1 = server_conn_string + '/' + db_name_1 + '?charset=utf8mb4'
engine_1 = db.create_engine(db_conn_string_1)
# if db_utils.database_exists(db_conn_string_1):
#     db_utils.drop_database(db_conn_string_1)
# db_utils.create_database(db_conn_string_1)
connection_1 = engine_1.connect()

m = db.MetaData()

# add all entities here
entity_msd_artist = db.Table(
    'msd_artist', m,
    db.Column('artist_id', db.CHAR(18), primary_key=True),
    db.Column('artist_mbid', db.CHAR(36)),
    db.Column('artist_name', db.VARCHAR(200)),
    db.Column('artist_lat', db.FLOAT),
    db.Column('artist_long', db.FLOAT)
)
entity_msd_artist_similarity = db.Table(
    'msd_artist_similarity', m,
    db.Column('artist_similarity_id', db.INTEGER, primary_key=True, autoincrement=True),
    db.Column('artist_id_1', db.CHAR(18)),
    db.Column('artist_id_2', db.CHAR(18))
)
entity_msd_r_term = db.Table(
    'msd_r_term', m,
    db.Column('term_id', db.INTEGER, primary_key=True, autoincrement=True),
    db.Column('term_name', db.VARCHAR(50))
)
entity_msd_r_mbtag = db.Table(
    'msd_r_mbtag', m,
    db.Column('mbtag_id', db.INTEGER, primary_key=True, autoincrement=True),
    db.Column('mbtag_name', db.VARCHAR(100))
)
entity_msd_artist_term = db.Table(
    'msd_artist_term', m,
    db.Column('artist_term_id', db.INTEGER, primary_key=True, autoincrement=True),
    db.Column('artist_id', db.CHAR(18)),
    db.Column('term_name', db.VARCHAR(50))
)
entity_msd_artist_mbtag = db.Table(
    'msd_artist_mbtag', m,
    db.Column('artist_mbtag_id', db.INTEGER, primary_key=True, autoincrement=True),
    db.Column('artist_id', db.CHAR(18)),
    db.Column('mbtag_name', db.VARCHAR(100))
)
entity_msd_track = db.Table(
    'msd_track', m,
    db.Column('track_id', db.CHAR(18), primary_key=True),
    db.Column('analysis_sample_rate', db.INTEGER),
    db.Column('audio_md5', db.CHAR(32)),
    db.Column('danceability', db.FLOAT),
    db.Column('duration', db.FLOAT),
    db.Column('end_of_fade_in', db.FLOAT),
    db.Column('energy', db.FLOAT),
    db.Column('key', db.INTEGER),
    db.Column('key_confidence', db.FLOAT),
    db.Column('loudness', db.FLOAT),
    db.Column('mode', db.FLOAT),
    db.Column('mode_confidence', db.FLOAT),
    db.Column('start_of_fade_out', db.FLOAT),
    db.Column('tempo', db.FLOAT),
    db.Column('time_signature', db.FLOAT),
    db.Column('time_signature_confidence', db.FLOAT),
    db.Column('artist_id', db.CHAR(18)),
    db.Column('release', db.VARCHAR(200)),
    db.Column('song_hotness', db.FLOAT),
    db.Column('title', db.VARCHAR(200)),
    db.Column('track_7digitalid', db.CHAR(7)),
    db.Column('year', db.INTEGER),
    db.Column('upload_timestamp', db.DATETIME)
)
entity_msd_bar = db.Table(
    'msd_bar', m,
    db.Column('bar_id', db.INTEGER, primary_key=True, autoincrement=True),
    db.Column('track_id', db.CHAR(18)),
    db.Column('track_bar_id', db.INTEGER),
    db.Column('bar_confidence', db.FLOAT),
    db.Column('bar_start', db.FLOAT)
)
entity_msd_beat = db.Table(
    'msd_beat', m,
    db.Column('beat_id', db.INTEGER, primary_key=True, autoincrement=True),
    db.Column('track_id', db.CHAR(18)),
    db.Column('track_beat_id', db.INTEGER),
    db.Column('beat_confidence', db.FLOAT),
    db.Column('beat_start', db.FLOAT)
)
entity_msd_section = db.Table(
    'msd_section', m,
    db.Column('section_id', db.INTEGER, primary_key=True, autoincrement=True),
    db.Column('track_id', db.CHAR(18)),
    db.Column('track_section_id', db.INTEGER),
    db.Column('section_confidence', db.FLOAT),
    db.Column('section_start', db.FLOAT)
)
entity_msd_tatum = db.Table(
    'msd_tatum', m,
    db.Column('tatum_id', db.INTEGER, primary_key=True, autoincrement=True),
    db.Column('track_id', db.CHAR(18)),
    db.Column('track_tatum_id', db.INTEGER),
    db.Column('tatum_confidence', db.FLOAT),
    db.Column('tatum_start', db.FLOAT)
)
entity_msd_segment = db.Table(
    'msd_segment', m,
    db.Column('segment_id', db.INTEGER, primary_key=True, autoincrement=True),
    db.Column('track_id', db.CHAR(18)),
    db.Column('track_segment_id', db.INTEGER),
    db.Column('segment_confidence', db.FLOAT),
    db.Column('segment_start', db.FLOAT),
    db.Column('segment_loudness_max', db.FLOAT),
    db.Column('segment_loudness_max_time', db.FLOAT),
    db.Column('segment_loudness_start', db.FLOAT),
    db.Column('p1', db.FLOAT),
    db.Column('p2', db.FLOAT),
    db.Column('p3', db.FLOAT),
    db.Column('p4', db.FLOAT),
    db.Column('p5', db.FLOAT),
    db.Column('p6', db.FLOAT),
    db.Column('p7', db.FLOAT),
    db.Column('p8', db.FLOAT),
    db.Column('p9', db.FLOAT),
    db.Column('p10', db.FLOAT),
    db.Column('p11', db.FLOAT),
    db.Column('p12', db.FLOAT),
    db.Column('t1', db.FLOAT),
    db.Column('t2', db.FLOAT),
    db.Column('t3', db.FLOAT),
    db.Column('t4', db.FLOAT),
    db.Column('t5', db.FLOAT),
    db.Column('t6', db.FLOAT),
    db.Column('t7', db.FLOAT),
    db.Column('t8', db.FLOAT),
    db.Column('t9', db.FLOAT),
    db.Column('t10', db.FLOAT),
    db.Column('t11', db.FLOAT),
    db.Column('t12', db.FLOAT)
)
m.create_all(engine_1)

#
# # transfer data ################################
# # date_param = datetime.datetime(2020, 12, 20)
# date_param_string_begin = "'2010-01-01 00:00:00'"
# date_param_string_end = "'2020-12-20 00:00:00'"
#
# # msd_track
# query_string = db.sql.text(
#     f'SELECT * FROM msd_track WHERE upload_timestamp > {date_param_string_begin} AND upload_timestamp < {date_param_string_end}'
# )
# results = connection_0.execute(query_string).fetchall()
# values_list = [{
#     'track_id': el[0], 'analysis_sample_rate': el[1], 'audio_md5': el[2],
#     'danceability': el[3], 'duration': el[4], 'end_of_fade_in': el[5],
#     'energy': el[6], 'key': el[7], 'key_confidence': el[8], 'loudness': el[9],
#     'mode': el[10], 'mode_confidence': el[11], 'start_of_fade_out': el[12], 'tempo': el[13],
#     'time_signature': el[14], 'time_signature_confidence': el[15], 'artist_id': el[16],
#     'release': el[17], 'song_hotness': el[18], 'title': el[19], 'track_7digitalid': el[20],
#     'year': el[21], 'upload_timestamp': el[22]
# } for el in results]
# query = db.insert(entity_msd_track).values(values_list).prefix_with('IGNORE')
# ResultProxy = connection_1.execute(query)
#
# # msd_segment-
# query_string = db.sql.text(
#     f'SELECT * FROM msd_segment WHERE track_id IN (SELECT track_id FROM msd_track WHERE upload_timestamp > {date_param_string_begin} AND upload_timestamp < {date_param_string_end})'
# )
# results = connection_0.execute(query_string).fetchall()
# values_list = [{'segment_id': el[0], 'track_id': el[1], 'track_segment_id': el[2],
#                     'segment_confidence': el[3], 'segment_start': el[4],
#                     'segment_loudness_max': el[5], 'segment_loudness_max_time': el[6],
#                     'segment_loudness_start': el[7],
#                     'p1':el[8],
#                     'p2':el[9],
#                     'p3': el[10],
#                     'p4': el[11],
#                     'p5': el[12],
#                     'p6': el[13],
#                     'p7': el[14],
#                     'p8': el[15],
#                     'p9': el[16],
#                     'p10': el[17],
#                     'p11': el[18],
#                     'p12': el[19],
#                     't1': el[20],
#                     't2': el[21],
#                     't3': el[22],
#                     't4': el[23],
#                     't5': el[24],
#                     't6': el[25],
#                     't7': el[26],
#                     't8': el[27],
#                     't9': el[28],
#                     't10': el[29],
#                     't11': el[30],
#                     't12': el[31]
#                     } for el in results]
# query = db.insert(entity_msd_segment).values(values_list).prefix_with('IGNORE')
# ResultProxy = connection_1.execute(query)
#
# # msd_bar-
# query_string = db.sql.text(
#     f'SELECT * FROM msd_bar WHERE track_id IN (SELECT track_id FROM msd_track WHERE upload_timestamp > {date_param_string_begin} AND upload_timestamp < {date_param_string_end})'
# )
# results = connection_0.execute(query_string).fetchall()
# values_list = [{'bar_id': el[0], 'track_id': el[1], 'track_bar_id': el[2],
#                 'bar_confidence': el[3], 'bar_start': el[4]} for el in results]
# query = db.insert(entity_msd_bar).values(values_list).prefix_with('IGNORE')
# ResultProxy = connection_1.execute(query)
#
# # msd_beat-
# query_string = db.sql.text(
#     f'SELECT * FROM msd_beat WHERE track_id IN (SELECT track_id FROM msd_track WHERE upload_timestamp > {date_param_string_begin} AND upload_timestamp < {date_param_string_end})'
# )
# results = connection_0.execute(query_string).fetchall()
# values_list = [{'beat_id': el[0], 'track_id': el[1], 'track_beat_id': el[2],
#                 'beat_confidence': el[3], 'beat_start': el[4]} for el in results]
# query = db.insert(entity_msd_beat).values(values_list).prefix_with('IGNORE')
# ResultProxy = connection_1.execute(query)
#
# # msd_section
# query_string = db.sql.text(
#     f'SELECT * FROM msd_section WHERE track_id IN (SELECT track_id FROM msd_track WHERE upload_timestamp > {date_param_string_begin} AND upload_timestamp < {date_param_string_end})'
# )
# results = connection_0.execute(query_string).fetchall()
# values_list = [{'section_id': el[0], 'track_id': el[1], 'track_section_id': el[2],
#                 'section_confidence': el[3], 'section_start': el[4]} for el in results]
# query = db.insert(entity_msd_section).values(values_list).prefix_with('IGNORE')
# ResultProxy = connection_1.execute(query)
#
# # msd_tatum-
# query_string = db.sql.text(
#     f'SELECT * FROM msd_tatum WHERE track_id IN (SELECT track_id FROM msd_track WHERE upload_timestamp > {date_param_string_begin} AND upload_timestamp < {date_param_string_end})'
# )
# results = connection_0.execute(query_string).fetchall()
# values_list = [{'tatum_id': el[0], 'track_id': el[1], 'track_tatum_id': el[2],
#                 'tatum_confidence': el[3], 'tatum_start': el[4]} for el in results]
# query = db.insert(entity_msd_tatum).values(values_list).prefix_with('IGNORE')
# ResultProxy = connection_1.execute(query)





connection_0.close()
connection_1.close()