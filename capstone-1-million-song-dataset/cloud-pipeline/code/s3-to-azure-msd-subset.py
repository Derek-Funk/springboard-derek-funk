# https://code.visualstudio.com/docs/remote/ssh-tutorial

# local:
# $ scp s3-to-azure-msd-subset.py derek-funk@<vm_ip>:/home/derek-funk/s3-to-azure-msd-subset.py

# remote:
# $ sudo su
# $$ curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# $$ curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# $$ exit
# $ sudo apt-get update
# $ sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
# $ sudo apt-get install python3-pip
# $ sudo apt-get install unixodbc-dev
# $ pip3 install h5py numpy pandas sqlalchemy==1.3.23 sqlalchemy_utils s3fs
# install extension ms-python.python

# azure:
# add <vm_ip> to sql server firewall
# currently using Azure VM of size "Standard B1ms" = 1vCPU, 2GiB RAM

import datetime
import h5json
import h5py
import math
import numpy as np
import pandas as pd
import pyodbc
import s3fs
import sqlalchemy as db
import sqlalchemy_utils as db_utils
import sqlite3
import string
import urllib
import sys #test
import time #test

class S3_to_azure_msd_subset:
    def __init__(self):
        # general
        self.batch_size = 75000 #75000 works, 100000 too much
        self.home_path = '/home/derek-funk/'
        self.metadata = db.MetaData()
        # Azure
        self.engine = None
        self.connection = None
        # S3
        self.s3 = s3fs.S3FileSystem(key='AKIAIVQFVOQOVUU6BGQA', secret='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')
        self.bucket = "bucket-msd-subset"
        # schemas - non-track data
        self.schema_msd_artist = db.Table(
            'msd_artist', self.metadata,
            db.Column('artist_id', db.CHAR(18), primary_key=True),
            db.Column('artist_mbid', db.CHAR(36)),
            db.Column('artist_name', db.VARCHAR(350)),
            db.Column('artist_lat', db.FLOAT),
            db.Column('artist_long', db.FLOAT)
        )
        self.schema_msd_artist_mbtag = db.Table(
            'msd_artist_mbtag', self.metadata,
            db.Column('artist_mbtag_id', db.INTEGER, primary_key=True, autoincrement=True),
            db.Column('artist_id', db.CHAR(18)),
            db.Column('mbtag_name', db.VARCHAR(150))
        )
        self.schema_msd_artist_similarity = db.Table(
            'msd_artist_similarity', self.metadata,
            db.Column('artist_similarity_id', db.INTEGER, primary_key=True, autoincrement=True),
            db.Column('artist_id_1', db.CHAR(18)),
            db.Column('artist_id_2', db.CHAR(18))
        )
        self.schema_msd_artist_term = db.Table(
            'msd_artist_term', self.metadata,
            db.Column('artist_term_id', db.INTEGER, primary_key=True, autoincrement=True),
            db.Column('artist_id', db.CHAR(18)),
            db.Column('term_name', db.VARCHAR(50))
        )
        self.schema_msd_r_mbtag = db.Table(
            'msd_r_mbtag', self.metadata,
            db.Column('mbtag_id', db.INTEGER, primary_key=True, autoincrement=True),
            db.Column('mbtag_name', db.VARCHAR(150))
        )
        self.schema_msd_r_term = db.Table(
            'msd_r_term', self.metadata,
            db.Column('term_id', db.INTEGER, primary_key=True, autoincrement=True),
            db.Column('term_name', db.VARCHAR(50))
        )
        # schemas - track data
        self.schema_msd_bar = db.Table(
            'msd_bar', self.metadata,
            db.Column('bar_id', db.INTEGER, primary_key=True, autoincrement=True),
            db.Column('track_id', db.CHAR(18)),
            db.Column('track_bar_id', db.INTEGER),
            db.Column('bar_confidence', db.FLOAT),
            db.Column('bar_start', db.FLOAT)
        )
        self.schema_msd_beat = db.Table(
            'msd_beat', self.metadata,
            db.Column('beat_id', db.INTEGER, primary_key=True, autoincrement=True),
            db.Column('track_id', db.CHAR(18)),
            db.Column('track_beat_id', db.INTEGER),
            db.Column('beat_confidence', db.FLOAT),
            db.Column('beat_start', db.FLOAT)
        )
        self.schema_msd_section = db.Table(
            'msd_section', self.metadata,
            db.Column('section_id', db.INTEGER, primary_key=True, autoincrement=True),
            db.Column('track_id', db.CHAR(18)),
            db.Column('track_section_id', db.INTEGER),
            db.Column('section_confidence', db.FLOAT),
            db.Column('section_start', db.FLOAT)
        )
        self.schema_msd_segment = db.Table(
            'msd_segment', self.metadata,
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
        self.schema_msd_tatum = db.Table(
            'msd_tatum', self.metadata,
            db.Column('tatum_id', db.INTEGER, primary_key=True, autoincrement=True),
            db.Column('track_id', db.CHAR(18)),
            db.Column('track_tatum_id', db.INTEGER),
            db.Column('tatum_confidence', db.FLOAT),
            db.Column('tatum_start', db.FLOAT)
        )
        self.schema_msd_track = db.Table(
            'msd_track', self.metadata,
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

    def connect_to_azure_sql(self):
        try:
            driver = '{ODBC Driver 17 for SQL Server}'
            server = 'azure-sql-server-msd.database.windows.net'
            database = 'msd-subset'
            user = 'derekfunk'
            password = <azure-sql-password>

            conn_str_suffix = 'Driver={};Server=tcp:{},1433;Database={};Uid={};Pwd={};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;' \
                .format(driver, server, database, user, password)
            conn_str_suffix_formatted = urllib.parse.quote_plus(conn_str_suffix)
            full_conn_str = 'mssql+pyodbc:///?autocommit=false&odbc_connect={}'.format(conn_str_suffix_formatted)
            self.engine = db.create_engine(full_conn_str, echo=True, fast_executemany=False)
            self.connection = self.engine.connect()
        except:
            raise Exception('Unable to connect to Azure SQL')
        return 'Connected to Azure SQL'

    def create_tables(self):
        try:
            self.metadata.create_all(self.engine)
        except:
            raise Exception('Unable to create tables')
        return 'Tables created'

    def drop_tables(self):
        try:
            self.engine.execute('''DROP TABLE IF EXISTS msd_artist, msd_artist_mbtag, msd_artist_similarity,
                msd_artist_term, msd_bar, msd_beat, msd_r_mbtag, msd_r_term, msd_section, msd_segment, msd_tatum, msd_track''')
        except:
            raise Exception('Unable to drop tables')
        return 'Database is now empty'
        
    def _insert_msd_artist(self):
        table_name = 'msd_artist'
        try:
            key = 'AdditionalFiles/unique_artists.txt'
            artists = pd.read_csv(self.s3.open('{}/{}'.format(self.bucket, key), mode='rb'), \
                sep='<SEP>', header=None, usecols=[0,1,3], engine='python')
            artists.columns = artists.columns.astype(str)

            key2 = 'AdditionalFiles/artist_location.txt'
            locations = pd.read_csv(self.s3.open('{}/{}'.format(self.bucket, key2), mode='rb'), \
                sep='<SEP>', header=None, engine='python')
            locations.columns = locations.columns.astype(str)

            artists_with_location = artists.merge(locations, on=['0'], how='left').iloc[:,:5]
            artists_with_location_clean = artists_with_location.where(pd.notnull(artists_with_location), None)
            data_msd_artist = np.array(artists_with_location_clean)

            no_batches = int(len(data_msd_artist) / self.batch_size) + 1
            for i in range(no_batches):
                query = db.insert(self.schema_msd_artist)
                values_list = [{'artist_id':el[0], 'artist_mbid':el[1], 'artist_name':el[2],
                                'artist_lat':el[3], 'artist_long':el[4]} for el in data_msd_artist[i*self.batch_size:(i+1)*self.batch_size,:]]
                ResultProxy = self.connection.execute(query, values_list)
        except:
            raise Exception('Unable to insert into {}'.format(table_name))
        return 'Added rows to {}'.format(table_name)
    
    def _insert_msd_artist_mbtag_and_msd_artist_term(self):
        table_name = 'msd_artist_mbtag and msd_artist_term'
        try:
            key = 'AdditionalFiles/artist_term.db'
            self.s3.download('{}/{}'.format(self.bucket, key), self.home_path)

            key2 = 'artist_term.db'
            sqlite_conn = sqlite3.connect(key2)
            sqlite_cursor = sqlite_conn.cursor()
            sqlite_cursor.execute('SELECT * FROM artist_mbtag;')
            data_msd_artist_mbtag = sqlite_cursor.fetchall()
            sqlite_cursor.execute('SELECT * FROM artist_term;')
            data_msd_artist_term = sqlite_cursor.fetchall()
            sqlite_conn.close()

            bad_values = ['1 13 165900 150 7672 22647 34612 48720 59280 74602 87545 95495 107182 131087 141522 153710', '1 7 186240 183 23558 41608 89158 111733 150833 169883']
            data_msd_artist_mbtag = [el for el in data_msd_artist_mbtag if el[1] not in bad_values]

            no_batches = int(len(data_msd_artist_mbtag) / self.batch_size) + 1
            for i in range(no_batches):
                query = db.insert(self.schema_msd_artist_mbtag)
                values_list = [{'artist_id':el[0], 'mbtag_name':el[1]} for el in data_msd_artist_mbtag[i*self.batch_size:(i+1)*self.batch_size]]
                ResultProxy = self.connection.execute(query, values_list)

            no_batches = int(len(data_msd_artist_term) / self.batch_size) + 1
            for i in range(no_batches):
                query = db.insert(self.schema_msd_artist_term)
                values_list = [{'artist_id':el[0], 'term_name':el[1]} for el in data_msd_artist_term[i*self.batch_size:(i+1)*self.batch_size]]
                ResultProxy = self.connection.execute(query, values_list)
        except:
            raise Exception('Unable to insert into {}'.format(table_name))
        return 'Added rows to {}'.format(table_name)

    def _insert_msd_artist_similarity(self):
        table_name = 'msd_artist_similarity'
        try:
            key = 'AdditionalFiles/artist_similarity.db'
            self.s3.download('{}/{}'.format(self.bucket, key), self.home_path)

            key2 = 'artist_similarity.db'
            sqlite_conn = sqlite3.connect(key2)
            sqlite_cursor = sqlite_conn.cursor()
            sqlite_cursor.execute('SELECT * FROM similarity;')
            data_msd_artist_similarity = sqlite_cursor.fetchall()
            sqlite_conn.close()

            no_batches = int(len(data_msd_artist_similarity) / self.batch_size) + 1
            for i in range(no_batches):
                query = db.insert(self.schema_msd_artist_similarity)
                values_list = [{'artist_id_1':el[0], 'artist_id_2':el[1]} for el in data_msd_artist_similarity[i*self.batch_size:(i+1)*self.batch_size]]
                ResultProxy = self.connection.execute(query, values_list)
        except:
            raise Exception('Unable to insert into {}'.format(table_name))
        return 'Added rows to {}'.format(table_name)

    def _insert_msd_r_mbtag(self):
        table_name = 'msd_r_mbtag'
        try:
            key = 'AdditionalFiles/unique_mbtags.txt'
            idx_bad_data = [2,3]
            data_msd_r_mbtag = pd.read_csv(self.s3.open('{}/{}'.format(self.bucket, key), mode='rb'), \
                sep='<SEP>', header=None, skiprows=idx_bad_data, engine='python')

            no_batches = int(len(data_msd_r_mbtag) / self.batch_size) + 1
            for i in range(no_batches):
                query = db.insert(self.schema_msd_r_mbtag)
                values_list = [{'mbtag_name':el.item()} for el in data_msd_r_mbtag.values[i*self.batch_size:(i+1)*self.batch_size]]
                ResultProxy = self.connection.execute(query, values_list)
        except:
            raise Exception('Unable to insert into {}'.format(table_name))
        return 'Added rows to {}'.format(table_name)

    def _insert_msd_r_term(self):
        table_name = 'msd_r_term'
        try:
            key = 'AdditionalFiles/unique_terms.txt'
            data_msd_r_term = pd.read_csv(self.s3.open('{}/{}'.format(self.bucket, key), mode='rb'), \
                header=None, engine='python')

            no_batches = int(len(data_msd_r_term) / self.batch_size) + 1
            for i in range(no_batches):
                query = db.insert(self.schema_msd_r_term)
                values_list = [{'term_name':el.item()} for el in data_msd_r_term.values[i*self.batch_size:(i+1)*self.batch_size]]
                ResultProxy = self.connection.execute(query, values_list)
        except:
            raise Exception('Unable to insert into {}'.format(table_name))
        return 'Added rows to {}'.format(table_name)

    # takes 75 min with batch size 75,000 (10,251,865 data points / 156 MB) <- I CAN DEAL WITH THAT
    def insert_non_track_data(self):
        self._insert_msd_artist() #44,745 rows - 62 s
        self._insert_msd_artist_mbtag_and_msd_artist_term() #24,775 and 1,109,381 rows
        self._insert_msd_artist_similarity() #2,201,916 rows
        self._insert_msd_r_mbtag() #2,319 rows
        self._insert_msd_r_term() #7,643 rows
    # select 
	# (select count(1) from dbo.msd_artist) as msd_artist,
	# (select count(1) from dbo.msd_artist_mbtag) as msd_artist_mbtag,
	# (select count(1) from dbo.msd_artist_term) as msd_artist_term,
	# (select count(1) from dbo.msd_artist_similarity) as msd_artist_similarity,
	# (select count(1) from dbo.msd_r_mbtag) as msd_r_mbtag,
	# (select count(1) from dbo.msd_r_term) as msd_r_term

    # takes 36 min for just 10 folders by batching per folder <- NO GOOD
    def insert_track_data(self, idx_first_folder=1, idx_last_folder=10):
        table_name = 'msd_bar, msd_beat, msd_section, msd_segment, msd_tatum, and msd_track'
        alphabet = tuple(string.ascii_uppercase)
        folder_group_1 = [f'data/A/{level_2}/{level_3}/' for level_2 in alphabet for level_3 in alphabet]
        folder_group_2 = [f'data/B/{level_2}/{level_3}/' for level_2 in alphabet[:8] for level_3 in alphabet]
        folder_group_3 = [f'data/B/I/{level_3}/' for level_3 in alphabet[:10]]
        all_folders = folder_group_1 + folder_group_2 + folder_group_3 #894 folders
        folders_to_process = all_folders[idx_first_folder-1:idx_last_folder]

        try:
            data_msd_track = None
            data_msd_bar = None
            data_msd_beat = None
            data_msd_section = None
            data_msd_segment = None
            data_msd_tatum = None
            for folder in folders_to_process:
                for file in self.s3.ls('{}/{}'.format(self.bucket, folder)):
                    h5 = h5py.File(self.s3.open(file, mode='rb'))
                    track_id = file.split('/')[-1][:-3]

                    # track - 18 min
                    h5['analysis'].keys()
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

                    upload_timestamp = datetime.datetime.now()

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

                    if len(data_msd_track) > 10000 or folder == folders_to_process[-1]:
                        query = db.insert(self.schema_msd_track)
                        values_list = [{
                            'track_id': el[0], 'analysis_sample_rate': el[1], 'audio_md5': el[2],
                            'danceability': el[3], 'duration': el[4], 'end_of_fade_in': el[5],
                            'energy': el[6], 'key': el[7], 'key_confidence': el[8], 'loudness': el[9],
                            'mode': el[10], 'mode_confidence': el[11], 'start_of_fade_out': el[12], 'tempo': el[13],
                            'time_signature': el[14], 'time_signature_confidence': el[15], 'artist_id': el[16],
                            'release': el[17], 'song_hotness': el[18], 'title': el[19], 'track_7digitalid': el[20],
                            'year': el[21], 'upload_timestamp': el[22]
                        } for el in data_msd_track]
                        print(values_list)
                        ResultProxy = self.connection.execute(query, values_list)
                        data_msd_track = None

                    # bar - 58 min
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

                    if len(data_msd_bar) > 50000 or folder == folders_to_process[-1]:
                        query = db.insert(self.schema_msd_bar)
                        values_list = [{'track_id': el[0], 'track_bar_id': el[1],
                                        'bar_confidence': el[2], 'bar_start': el[3]} for el in data_msd_bar]
                        ResultProxy = self.connection.execute(query, values_list)
                        data_msd_bar = None

                    # beat - 2 hr and 23 min
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

                    if len(data_msd_beat) > 75000 or folder == folders_to_process[-1]:
                        query = db.insert(self.schema_msd_beat)
                        values_list = [{'track_id': el[0], 'track_beat_id': el[1],
                                        'beat_confidence': el[2], 'beat_start': el[3]} for el in data_msd_beat]
                        ResultProxy = self.connection.execute(query, values_list)
                        data_msd_beat = None

                    # section - 18 min
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

                    if len(data_msd_section) > 100000 or folder == folders_to_process[-1]:
                        query = db.insert(self.schema_msd_section)
                        values_list = [{'track_id': el[0], 'track_section_id': el[1],
                                        'section_confidence': el[2], 'section_start': el[3]} for el in data_msd_section]
                        ResultProxy = self.connection.execute(query, values_list)
                        data_msd_section = None

                    # tatum - 4 hr and 47 min
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

                    if len(data_msd_tatum) > 75000 or folder == folders_to_process[-1]:
                        query = db.insert(self.schema_msd_tatum)
                        values_list = [{'track_id': el[0], 'track_tatum_id': el[1],
                                        'tatum_confidence': el[2], 'tatum_start': el[3]} for el in data_msd_tatum]
                        ResultProxy = self.connection.execute(query, values_list)
                        data_msd_tatum = None

                    # segment - took too long, did not write (wrote 25% over a whole day)
                #     segments_confidence = np.array(h5['analysis/segments_confidence'])
                #     no_segments = len(segments_confidence)
                #     segments_start = np.array(h5['analysis/segments_start'])
                #     segments_loudness_max = np.array(h5['analysis/segments_loudness_max'])
                #     segments_loudness_max_time = np.array(h5['analysis/segments_loudness_max_time'])
                #     segments_loudness_start = np.array(h5['analysis/segments_loudness_start'])
                #     segments_pitch = np.array(h5['analysis/segments_pitches'])
                #     segments_timbre = np.array(h5['analysis/segments_timbre'])

                #     data_msd_segment_batch = np.column_stack((
                #         np.full((no_segments), track_id),
                #         np.array(range(1, no_segments + 1)),
                #         segments_confidence,
                #         segments_start,
                #         segments_loudness_max,
                #         segments_loudness_max_time,
                #         segments_loudness_start,
                #         segments_pitch,
                #         segments_timbre
                #     ))
                #     if data_msd_segment is None:
                #         data_msd_segment = data_msd_segment_batch
                #     else:
                #         data_msd_segment = np.row_stack((
                #             data_msd_segment,
                #             data_msd_segment_batch
                #         ))
                
                # if len(data_msd_segment) > 10000 or folder == folders_to_process[-1]:
                #     query = db.insert(self.schema_msd_segment)
                #     values_list = [{'track_id': el[0], 'track_segment_id': el[1],
                #                     'segment_confidence': el[2], 'segment_start': el[3],
                #                     'segment_loudness_max': el[4], 'segment_loudness_max_time': el[5],
                #                     'segment_loudness_start': el[6],
                #                     'p1':el[7],
                #                     'p2':el[8],
                #                     'p3': el[9],
                #                     'p4': el[10],
                #                     'p5': el[11],
                #                     'p6': el[12],
                #                     'p7': el[13],
                #                     'p8': el[14],
                #                     'p9': el[15],
                #                     'p10': el[16],
                #                     'p11': el[17],
                #                     'p12': el[18],
                #                     't1': el[19],
                #                     't2': el[20],
                #                     't3': el[21],
                #                     't4': el[22],
                #                     't5': el[23],
                #                     't6': el[24],
                #                     't7': el[25],
                #                     't8': el[26],
                #                     't9': el[27],
                #                     't10': el[28],
                #                     't11': el[29],
                #                     't12': el[30]
                #                     } for el in data_msd_segment]
                #     ResultProxy = self.connection.execute(query, values_list)
                #     data_msd_segment = None
        except:
            raise Exception('Unable to insert into {}'.format(table_name))
        return 'Added rows to {}'.format(table_name)
    # select 
	# (select count(1) from dbo.msd_track) as msd_track,
	# (select count(1) from dbo.msd_bar) as msd_bar,
	# (select count(1) from dbo.msd_beat) as msd_beat,
	# (select count(1) from dbo.msd_section) as msd_section,
	# (select count(1) from dbo.msd_tatum) as msd_tatum,
	# (select count(1) from dbo.msd_segment) as msd_segment

    def disconnect_azure_sql(self):
        try:
            self.connection.close()
            self.engine, self.connection = None, None
        except:
            raise Exception('Unable to disconnect from Azure SQL')
        return 'Disconnected from Azure SQL'

msd_subset = S3_to_azure_msd_subset()
msd_subset.connect_to_azure_sql()
msd_subset.create_tables()

t0 = time.time()
msd_subset.insert_non_track_data()
t1 = time.time()
print((t1-t0)/60)

t0 = time.time()
msd_subset.insert_track_data(idx_first_folder=1, idx_last_folder=894)
t1 = time.time()
print((t1-t0)/60)

# msd_subset.drop_tables()
msd_subset.disconnect_azure_sql()
