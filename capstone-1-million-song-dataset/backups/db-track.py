##### DESCRIPTION
# This creates the following tables:
# msd_artist - list of artists
# msd_artist_similarity - list of similarities between artists
# msd_r_term - list of possible terms (basically genres)
# msd_artist_term - list of artist-term pairings
# msd_r_mbtag - list of possible tags from music brainz (similar to genres)
# msd_artist_mbtag - list of artist-music brainz tags
# RUN THIS AFTER db-non-track.py
# 11/25/20 run #1: 41 minutes
# 11/25/20 run #2: 32 minutes

import h5py
import math
import numpy as np
import os
import pandas as pd
import sqlalchemy as db
import sqlalchemy_utils as db_utils
import string
import time

t0 = time.time()

alphabet = tuple(string.ascii_uppercase)

# this needs to change for full dataset
folder_group_1 = [f'MillionSongSubset/data/A/{level_2}/{level_3}/' for level_2 in alphabet for level_3 in alphabet]
folder_group_2 = [f'MillionSongSubset/data/B/{level_2}/{level_3}/' for level_2 in alphabet[:8] for level_3 in alphabet]
folder_group_3 = [f'MillionSongSubset/data/B/I/{level_3}/' for level_3 in alphabet[:10]]
folders = folder_group_1 + folder_group_2 + folder_group_3

##### DATABASE STEP
# DB connection set up
server = 'localhost'
database = 'songDB'
port = 3306
username = 'root'
password = 'Leanmeanandg!1'
server_conn_string = f'mysql+mysqldb://{username}:{password}@{server}:{port}'
db_name = 'msd' # million song dataset
db_conn_string = server_conn_string + '/' + db_name
engine = db.create_engine(db_conn_string)
connection = engine.connect()

# DDL create tables
m = db.MetaData()
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
    db.Column('year', db.INTEGER)
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

# DML insert in batches (1 batch per folder)

t1 = time.time()

# loop over each nested folder
for folder in folders:
    # clear out batches for next folder
    data_msd_track = None
    data_msd_bar = None
    data_msd_beat = None
    data_msd_section = None
    data_msd_tatum = None
    data_msd_segment = None
    # loop over files in this folder
    for file in os.listdir(folder):
        h5 = h5py.File(folder + file,'r')

        # top level
        track_id = file[:-3]

        # h5['analysis'].keys()
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

        # h5['metadata'].keys()
        artist_id = np.array(h5['metadata/songs'])[0][4]
        release = np.array(h5['metadata/songs'])[0][14]
        if math.isnan(np.array(h5['metadata/songs'])[0][16]):
            song_hotness = None
        else:
            song_hotness = np.array(h5['metadata/songs'])[0][16]
        title = np.array(h5['metadata/songs'])[0][18]
        track_7digitalid = np.array(h5['metadata/songs'])[0][19]

        # h5['musicbrainz'].keys()
        if np.array(h5['musicbrainz/songs'])[0][1] == 0:
            year = None
        else:
            year = np.array(h5['musicbrainz/songs'])[0][1]

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
            year
        ))

        if data_msd_track is None:
            data_msd_track = data_msd_track_batch
        else:
            data_msd_track = np.row_stack((
                data_msd_track,
                data_msd_track_batch
            ))

        # bar
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

        # beat
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

        # section
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

        # tatum
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

        # segment
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

    # write folder batch to DB
    query = db.insert(entity_msd_track)
    values_list = [{
        'track_id': el[0], 'analysis_sample_rate': el[1], 'audio_md5': el[2],
        'danceability': el[3], 'duration': el[4], 'end_of_fade_in': el[5],
        'energy': el[6], 'key': el[7], 'key_confidence': el[8], 'loudness': el[9],
        'mode': el[10], 'mode_confidence': el[11], 'start_of_fade_out': el[12], 'tempo': el[13],
        'time_signature': el[14], 'time_signature_confidence': el[15], 'artist_id': el[16],
        'release': el[17], 'song_hotness': el[18], 'title': el[19], 'track_7digitalid': el[20],
        'year': el[21]
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

t2 = time.time()
print(t2 - t1)

connection.close()