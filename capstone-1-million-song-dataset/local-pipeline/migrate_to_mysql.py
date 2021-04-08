import datetime
import h5py
import math
import numpy as np
import os
import pandas as pd
import random
import sqlalchemy as db
import sqlalchemy_utils as db_utils
import sqlite3
import string
import time

def migrate_to_mysql(mysql_server, mysql_username, mysql_password, mysql_port, mysql_db):
    t0 = time.time()
    print('\n' + '-' * 100, 'STAGE 2: MIGRATING DATA TO MYSQL', '-' * 100 + '\n', sep='\n')

    # add a check to see if mysql credentials can successfully make a connection

    print('\n' + '-' * 100, 'Processing raw data files...', '-' * 100 + '\n', sep='\n')
    filename_0 = 'MillionSongSubset/AdditionalFiles/subset_unique_artists.txt'
    artist_0 = pd.read_csv(filename_0, sep='<SEP>', header=None, usecols=[0,1,3])
    data_msd_r_term = pd.read_csv('MillionSongSubset/AdditionalFiles/subset_unique_terms.txt', header=None)
    idx_bad_data = [1,2]
    idx_foreign = [723,724,725]
    data_msd_r_mbtag = pd.read_csv('MillionSongSubset/AdditionalFiles/subset_unique_mbtags.txt', header=None, skiprows=idx_bad_data+idx_foreign)
    bad_values = ['1 13 165900 150 7672 22647 34612 48720 59280 74602 87545 95495 107182 131087 141522 153710',
    '1 7 186240 183 23558 41608 89158 111733 150833 169883',
                  'ਪੰਜਾਬੀ',
                  'ਭੰਗੜਾ',
                  '香港歌手'
                  ]

    filename_1 = 'MillionSongSubset/AdditionalFiles/subset_artist_location.txt'
    location_0 = pd.read_csv(filename_1, sep='<SEP>', header=None)
    artist_1 = artist_0.copy()
    artist_1['lat'] = np.nan
    artist_1['long'] = np.nan
    for artist_idx, artist_id in enumerate(artist_1.iloc[:, 0]):
        if artist_id in location_0.iloc[:, 0].values:
            loc_idx = list(location_0.iloc[:, 0]).index(artist_id)
            artist_1.iloc[artist_idx, 3] = location_0.iloc[loc_idx, 1]
            artist_1.iloc[artist_idx, 4] = location_0.iloc[loc_idx, 2]
    artist_2 = artist_1.where(pd.notnull(artist_1), None)
    data_msd_artist = np.array(artist_2)

    sqlite_db_1 = 'MillionSongSubset/AdditionalFiles/subset_artist_similarity.db'
    con = sqlite3.connect(sqlite_db_1)
    cur = con.cursor()
    cur.execute('SELECT * FROM similarity;')
    data_msd_artist_similarity = cur.fetchall()
    con.close()

    sqlite_db_2 = 'MillionSongSubset/AdditionalFiles/subset_artist_term.db'
    con = sqlite3.connect(sqlite_db_2)
    cur = con.cursor()
    cur.execute('SELECT * FROM artist_term;')
    data_msd_artist_term = cur.fetchall()
    cur.execute('SELECT * FROM artist_mbtag;')
    data_msd_artist_mbtag = cur.fetchall()
    con.close()

    # clean bad values
    data_msd_artist_mbtag = [el for el in data_msd_artist_mbtag if el[1] not in bad_values]

    print('\n' + '-' * 100, 'Creating tables in MySQL', '-' * 100 + '\n', sep='\n')
    db_conn_string = f'mysql+mysqldb://{mysql_username}:{mysql_password}@{mysql_server}:{mysql_port}/{mysql_db}'
    if db_utils.database_exists(db_conn_string):
        db_utils.drop_database(db_conn_string)
    db_utils.create_database(db_conn_string)

    engine = db.create_engine(db_conn_string)
    connection = engine.connect()

    m = db.MetaData()

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
    m.create_all(engine)

    print('\n' + '-' * 100, 'Populating MySQL tables with data...', '-' * 100 + '\n', sep='\n')
    query = db.insert(entity_msd_artist)
    values_list = [{'artist_id':el[0], 'artist_mbid':el[1], 'artist_name':el[2],
                    'artist_lat':el[3], 'artist_long':el[4]} for el in data_msd_artist]
    ResultProxy = connection.execute(query, values_list)

    query = db.insert(entity_msd_artist_similarity)
    values_list = [{'artist_id_1':el[0], 'artist_id_2':el[1]} for el in data_msd_artist_similarity]
    ResultProxy = connection.execute(query, values_list)

    query = db.insert(entity_msd_r_term)
    values_list = [{'term_name':el.item()} for el in data_msd_r_term.values]
    ResultProxy = connection.execute(query, values_list)

    query = db.insert(entity_msd_r_mbtag)
    values_list = [{'mbtag_name':el.item()} for el in data_msd_r_mbtag.values]
    ResultProxy = connection.execute(query, values_list)

    query = db.insert(entity_msd_artist_term)
    values_list = [{'artist_id':el[0], 'term_name':el[1]} for el in data_msd_artist_term]
    ResultProxy = connection.execute(query, values_list)

    query = db.insert(entity_msd_artist_mbtag)
    values_list = [{'artist_id':el[0], 'mbtag_name':el[1]} for el in data_msd_artist_mbtag]
    ResultProxy = connection.execute(query, values_list)

    timestamp_first = datetime.datetime(2010, 1, 1)
    timestamp_mid = datetime.datetime(2020, 12, 20)
    timestamp_last = datetime.datetime(2021, 2, 20)
    song_counter = 1

    alphabet = tuple(string.ascii_uppercase)
    folder_group_1 = [f'MillionSongSubset/data/A/{level_2}/{level_3}/' for level_2 in alphabet for level_3 in alphabet]
    folder_group_2 = [f'MillionSongSubset/data/B/{level_2}/{level_3}/' for level_2 in alphabet[:8] for level_3 in alphabet]
    folder_group_3 = [f'MillionSongSubset/data/B/I/{level_3}/' for level_3 in alphabet[:10]]
    folders = folder_group_1 + folder_group_2 + folder_group_3

    for folder in folders:
        tx = time.time()
        elapsed_time = round((tx - t0) / 60)
        print('\n' + '-' * 100, 'Data migration ongoing...', f'Migration time so far: {elapsed_time} minutes.', '-' * 100 + '\n', sep='\n')
        # maybe give an update on how many songs have been added so far

        data_msd_track = None
        data_msd_bar = None
        data_msd_beat = None
        data_msd_section = None
        data_msd_tatum = None
        data_msd_segment = None

        for file in os.listdir(folder):
            h5 = h5py.File(folder + file,'r')

            track_id = file[:-3]

            analysis_sample_rate = np.array(h5['analysis/songs'])[0][0]
            audio_md5 = np.array(h5['analysis/songs'])[0][1]
            if np.array(h5['analysis/songs'])[0][2] == 0:
                danceability = None
            else:
                danceability = np.array(h5['analysis/songs'])[0][2]
            duration = np.array(h5['analysis/songs'])[0][3]
            end_of_fade_in = np.array(h5['analysis/songs'])[0][4]
            if np.array(h5['analysis/songs'])[0][5] == 0:
                energy = None
            else:
                energy = np.array(h5['analysis/songs'])[0][5]
            key = np.array(h5['analysis/songs'])[0][21]
            key_confidence = np.array(h5['analysis/songs'])[0][22]
            loudness = np.array(h5['analysis/songs'])[0][23]
            mode = np.array(h5['analysis/songs'])[0][24]
            mode_confidence = np.array(h5['analysis/songs'])[0][25]
            start_of_fade_out = np.array(h5['analysis/songs'])[0][26]
            tempo = np.array(h5['analysis/songs'])[0][27]
            time_signature = np.array(h5['analysis/songs'])[0][28]
            time_signature_confidence = np.array(h5['analysis/songs'])[0][29]

            artist_id = np.array(h5['metadata/songs'])[0][4]
            release = np.array(h5['metadata/songs'])[0][14]
            if math.isnan(np.array(h5['metadata/songs'])[0][16]):
                song_hotness = None
            else:
                song_hotness = np.array(h5['metadata/songs'])[0][16]
            title = np.array(h5['metadata/songs'])[0][18]
            track_7digitalid = np.array(h5['metadata/songs'])[0][19]

            if np.array(h5['musicbrainz/songs'])[0][1] == 0:
                year = None
            else:
                year = np.array(h5['musicbrainz/songs'])[0][1]

            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            if song_counter <= 5000:
                duration_half = (timestamp_mid - timestamp_first).days
                day_diff = random.randint(0, duration_half)
                upload_timestamp = timestamp_first + datetime.timedelta(days=day_diff, hours=hour, minutes=minute, seconds=second)
            else:
                duration_half = (timestamp_last - timestamp_mid).days
                day_diff = random.randint(0, duration_half)
                upload_timestamp = timestamp_mid + datetime.timedelta(days=day_diff, hours=hour, minutes=minute, seconds=second)

            data_msd_track_batch = np.column_stack((
                track_id,
                analysis_sample_rate,
                audio_md5,
                danceability,
                duration,
                end_of_fade_in,
                energy,
                key,
                key_confidence,
                loudness,
                mode,
                mode_confidence,
                start_of_fade_out,
                tempo,
                time_signature,
                time_signature_confidence,
                artist_id,
                release,
                song_hotness,
                title,
                track_7digitalid,
                year,
                upload_timestamp
            ))

            if data_msd_track is None:
                data_msd_track = data_msd_track_batch
            else:
                data_msd_track = np.row_stack((
                    data_msd_track,
                    data_msd_track_batch
                ))

            bars_confidence = np.array(h5['analysis/bars_confidence'])
            no_bars = len(bars_confidence)
            bars_start = np.array(h5['analysis/bars_start'])
            data_msd_bar_batch = np.column_stack((
                np.full((no_bars), track_id),
                np.array(range(1, no_bars + 1)),
                bars_confidence,
                bars_start
            ))
            if data_msd_bar is None:
                data_msd_bar = data_msd_bar_batch
            else:
                data_msd_bar = np.row_stack((
                    data_msd_bar,
                    data_msd_bar_batch
                ))

            beats_confidence = np.array(h5['analysis/beats_confidence'])
            no_beats = len(beats_confidence)
            beats_start = np.array(h5['analysis/beats_start'])
            data_msd_beat_batch = np.column_stack((
                np.full((no_beats), track_id),
                np.array(range(1, no_beats + 1)),
                beats_confidence,
                beats_start
            ))
            if data_msd_beat is None:
                data_msd_beat = data_msd_beat_batch
            else:
                data_msd_beat = np.row_stack((
                    data_msd_beat,
                    data_msd_beat_batch
                ))

            sections_confidence = np.array(h5['analysis/sections_confidence'])
            no_sections = len(sections_confidence)
            sections_start = np.array(h5['analysis/sections_start'])
            data_msd_section_batch = np.column_stack((
                np.full((no_sections), track_id),
                np.array(range(1, no_sections + 1)),
                sections_confidence,
                sections_start
            ))
            if data_msd_section is None:
                data_msd_section = data_msd_section_batch
            else:
                data_msd_section = np.row_stack((
                    data_msd_section,
                    data_msd_section_batch
                ))

            tatums_confidence = np.array(h5['analysis/tatums_confidence'])
            no_tatums = len(tatums_confidence)
            tatums_start = np.array(h5['analysis/tatums_start'])
            data_msd_tatum_batch = np.column_stack((
                np.full((no_tatums), track_id),
                np.array(range(1, no_tatums + 1)),
                tatums_confidence,
                tatums_start
            ))
            if data_msd_tatum is None:
                data_msd_tatum = data_msd_tatum_batch
            else:
                data_msd_tatum = np.row_stack((
                    data_msd_tatum,
                    data_msd_tatum_batch
                ))

            segments_confidence = np.array(h5['analysis/segments_confidence'])
            no_segments = len(segments_confidence)
            segments_start = np.array(h5['analysis/segments_start'])
            segments_loudness_max = np.array(h5['analysis/segments_loudness_max'])
            segments_loudness_max_time = np.array(h5['analysis/segments_loudness_max_time'])
            segments_loudness_start = np.array(h5['analysis/segments_loudness_start'])
            segments_pitch = np.array(h5['analysis/segments_pitches'])
            segments_timbre = np.array(h5['analysis/segments_timbre'])

            data_msd_segment_batch = np.column_stack((
                np.full((no_segments), track_id),
                np.array(range(1, no_segments + 1)),
                segments_confidence,
                segments_start,
                segments_loudness_max,
                segments_loudness_max_time,
                segments_loudness_start,
                segments_pitch,
                segments_timbre
            ))
            if data_msd_segment is None:
                data_msd_segment = data_msd_segment_batch
            else:
                data_msd_segment = np.row_stack((
                    data_msd_segment,
                    data_msd_segment_batch
                ))

            song_counter += 1

        query = db.insert(entity_msd_track)
        values_list = [{
            'track_id': el[0], 'analysis_sample_rate': el[1], 'audio_md5': el[2],
            'danceability': el[3], 'duration': el[4], 'end_of_fade_in': el[5],
            'energy': el[6], 'key': el[7], 'key_confidence': el[8], 'loudness': el[9],
            'mode': el[10], 'mode_confidence': el[11], 'start_of_fade_out': el[12], 'tempo': el[13],
            'time_signature': el[14], 'time_signature_confidence': el[15], 'artist_id': el[16],
            'release': el[17], 'song_hotness': el[18], 'title': el[19], 'track_7digitalid': el[20],
            'year': el[21], 'upload_timestamp': el[22]
        } for el in data_msd_track]
        ResultProxy = connection.execute(query, values_list)

        query = db.insert(entity_msd_bar)
        values_list = [{'track_id': el[0], 'track_bar_id': el[1],
                        'bar_confidence': el[2], 'bar_start': el[3]} for el in data_msd_bar]
        ResultProxy = connection.execute(query, values_list)

        query = db.insert(entity_msd_beat)
        values_list = [{'track_id': el[0], 'track_beat_id': el[1],
                        'beat_confidence': el[2], 'beat_start': el[3]} for el in data_msd_beat]
        ResultProxy = connection.execute(query, values_list)

        query = db.insert(entity_msd_section)
        values_list = [{'track_id': el[0], 'track_section_id': el[1],
                        'section_confidence': el[2], 'section_start': el[3]} for el in data_msd_section]
        ResultProxy = connection.execute(query, values_list)

        query = db.insert(entity_msd_tatum)
        values_list = [{'track_id': el[0], 'track_tatum_id': el[1],
                        'tatum_confidence': el[2], 'tatum_start': el[3]} for el in data_msd_tatum]
        ResultProxy = connection.execute(query, values_list)

        query = db.insert(entity_msd_segment)
        values_list = [{'track_id': el[0], 'track_segment_id': el[1],
                        'segment_confidence': el[2], 'segment_start': el[3],
                        'segment_loudness_max': el[4], 'segment_loudness_max_time': el[5],
                        'segment_loudness_start': el[6],
                        'p1':el[7],
                        'p2':el[8],
                        'p3': el[9],
                        'p4': el[10],
                        'p5': el[11],
                        'p6': el[12],
                        'p7': el[13],
                        'p8': el[14],
                        'p9': el[15],
                        'p10': el[16],
                        'p11': el[17],
                        'p12': el[18],
                        't1': el[19],
                        't2': el[20],
                        't3': el[21],
                        't4': el[22],
                        't5': el[23],
                        't6': el[24],
                        't7': el[25],
                        't8': el[26],
                        't9': el[27],
                        't10': el[28],
                        't11': el[29],
                        't12': el[30]
                        } for el in data_msd_segment]
        ResultProxy = connection.execute(query, values_list)

    connection.close()

    t1 = time.time()
    download_time = round((t1 - t0) / 60)
    print('\n' + '-' * 100, 'END OF STAGE 2: MIGRATING DATA TO MYSQL', f'Stage 2 duration: {download_time} minutes.',
          '-' * 100 + '\n', sep='\n')
    return

# example usage
# migrate_to_mysql(mysql_server='localhost', mysql_username='root', mysql_password=<mysql_password>, mysql_port=3306, mysql_db='msd')
