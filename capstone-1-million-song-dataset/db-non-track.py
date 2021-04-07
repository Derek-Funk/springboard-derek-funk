##### DESCRIPTION
# This creates the following tables:
# msd_artist - list of artists
# msd_artist_similarity - list of similarities between artists
# msd_r_term - list of possible terms (basically genres)
# msd_artist_term - list of artist-term pairings
# msd_r_mbtag - list of possible tags from music brainz (similar to genres)
# msd_artist_mbtag - list of artist-music brainz tags
# RUN THIS BEFORE db-track.py
# ONLY TAKES A FEW SECONDS FOR 10,000 SONGS

import h5py
import numpy as np
import pandas as pd
import sqlalchemy as db
import sqlalchemy_utils as db_utils
import sqlite3

##### subset_unique_tracks.txt
# SKIP - we will get these from h5 files in the data folder
# pd.read_csv('MillionSongSubset/AdditionalFiles/subset_unique_tracks.txt')

##### subset_unique_artists.txt
# start with list of unique artists
filename_0 = 'MillionSongSubset/AdditionalFiles/subset_unique_artists.txt'
artist_0 = pd.read_csv(filename_0, sep='<SEP>', header=None, usecols=[0,1,3])

##### subset_unique_terms.txt
data_msd_r_term = pd.read_csv('MillionSongSubset/AdditionalFiles/subset_unique_terms.txt', header=None)

##### subset_unique_mbtags.txt
# skipped some bad data and some foreign languages
idx_bad_data = [1,2]
idx_foreign = [723,724,725]
data_msd_r_mbtag = pd.read_csv('MillionSongSubset/AdditionalFiles/subset_unique_mbtags.txt', header=None, skiprows=idx_bad_data+idx_foreign)
bad_values = ['1 13 165900 150 7672 22647 34612 48720 59280 74602 87545 95495 107182 131087 141522 153710',
'1 7 186240 183 23558 41608 89158 111733 150833 169883',
              'ਪੰਜਾਬੀ',
              'ਭੰਗੜਾ',
              '香港歌手'
              ]

##### subset_artist_location.txt
# add lat/long of artists
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

##### subset_artist_similaritu.db
# no tables in this db - SKIP
# sqlite_db_0 = 'MillionSongSubset/AdditionalFiles/subset_artist_similaritu.db'
# con = sqlite3.connect(sqlite_db_0)
# cur = con.cursor()
# cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cur.fetchall())
# con.close()

##### subset_artist_similarity.db
# one table worth getting: similarity
# sqlite_db_1 = 'MillionSongSubset/AdditionalFiles/subset_artist_similarity.db'
# con = sqlite3.connect(sqlite_db_1)
# cur = con.cursor()
# cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cur.fetchall())
# cur.execute('SELECT * FROM similarity;')
# data_msd_artist_similarity = cur.fetchall()
# con.close()

##### subset_artist_term.db
# 2 tables worth getting: artist_term, artist_mbtag
# sqlite_db_2 = 'MillionSongSubset/AdditionalFiles/subset_artist_term.db'
# con = sqlite3.connect(sqlite_db_2)
# cur = con.cursor()
# cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cur.fetchall())
# # already have artists from subset_unique_artists.txt
# # already have terms from subset_unique_terms.txt
# # already have mbtags from subset_unique_mbtags.txt
# cur.execute('SELECT * FROM terms;')
# print(cur.fetchall())
# cur.execute('SELECT * FROM artist_term;')
# data_msd_artist_term = cur.fetchall()
# cur.execute('SELECT * FROM artist_mbtag;')
# data_msd_artist_mbtag = cur.fetchall()
# con.close()

# clean bad values
data_msd_artist_mbtag = [el for el in data_msd_artist_mbtag if el[1] not in bad_values]

# this is taking too slow
# data_msd_artist_term = [(el[0], list(data_msd_r_term.values).index(el[1])) for el in data_msd_artist_term_0]

##### subset_msd_summary
# SKIP - we will get this info and more from h5 files
# file_name = 'MillionSongSubset/AdditionalFiles/subset_msd_summary_file.h5'
# h5 = h5py.File(file_name,'r')
# h5.keys()
# h5['analysis'].keys()
# np.array(h5['analysis/songs'])
# h5['metadata'].keys()
# np.array(h5['metadata/songs'])
# h5['musicbrainz'].keys()
# np.array(h5['musicbrainz/songs'])

##### subset_track_metadata.db
# SKIP - we will get this info and more from h5 files
# sqlite_db_2 = 'MillionSongSubset/AdditionalFiles/subset_track_metadata.db'
# con = sqlite3.connect(sqlite_db_2)
# cur = con.cursor()
# cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cur.fetchall())
# # already have artists from subset_unique_artists.txt
# # already have terms from subset_unique_terms.txt
# # already have mbtags from subset_unique_mbtags.txt
# cur.execute('SELECT * FROM songs;')
# x = cur.fetchall()
# print(cur.fetchall())
# con.close()

##### subset_unique_terms.txt
# SKIP - we will get this info and more from h5 files
# pd.read_csv('MillionSongSubset/AdditionalFiles/subset_tracks_per_year.txt', sep='<SEP>', header=None)

################################################################################################
##### DATABASE STEP
# put all python objects together and ready them for loading into DB
# https://towardsdatascience.com/sqlalchemy-python-tutorial-79a577141a91
# https://sqlalchemy-utils.readthedocs.io/en/latest/database_helpers.html

server = 'localhost'
database = 'songDB'
port = 3306
username = 'root'
password = 'Leanmeanandg!1'

###
# conda install mysqlclient
server_conn_string = f'mysql+mysqldb://{username}:{password}@{server}:{port}'
engine = db.create_engine(server_conn_string)

db_name = 'msd_0' # million song dataset
db_conn_string = server_conn_string + '/' + db_name
if db_utils.database_exists(db_conn_string):
    db_utils.drop_database(db_conn_string)
db_utils.create_database(db_conn_string)

engine = db.create_engine(db_conn_string)
connection = engine.connect()

m = db.MetaData()

# add new entities here
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

m.create_all(engine)

# add new inserts here
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

connection.close()