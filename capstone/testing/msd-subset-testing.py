# remote:
# $ pip3 install pytest

import sqlalchemy as db
import urllib

try:
    driver = '{ODBC Driver 17 for SQL Server}'
    server = 'azure-sql-server-msd.database.windows.net'
    database = 'msd-subset'
    user = 'derekfunk'
    password = 'Leanmeanandg!1'

    conn_str_suffix = 'Driver={};Server=tcp:{},1433;Database={};Uid={};Pwd={};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;' \
        .format(driver, server, database, user, password)
    conn_str_suffix_formatted = urllib.parse.quote_plus(conn_str_suffix)
    full_conn_str = 'mssql+pyodbc:///?autocommit=false&odbc_connect={}'.format(conn_str_suffix_formatted)
    engine = db.create_engine(full_conn_str, echo=True, fast_executemany=False)
except:
    raise Exception('Unable to connect to Azure SQL')

def test_msd_bar():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_bar'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 1649792
    
    connection.close()

def test_msd_section():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_section'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 99897
    
    connection.close()

def test_msd_tatum():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_tatum'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 10479481
    
    connection.close()

def test_msd_artist_term():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_artist_term'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 1109381
    
    connection.close()

def test_msd_artist_similarity():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_artist_similarity'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 2201916
    
    connection.close()

def test_msd_track():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_track'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 10000
    
    connection.close()

def test_msd_artist():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_artist'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 44745
    
    connection.close()

def test_msd_beat():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_beat'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 4809945
    
    connection.close()

def test_msd_artist_mbtag():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_artist_mbtag'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 24775
    
    connection.close()

def test_msd_r_term():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_r_term'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 7643
    
    connection.close()

def test_msd_r_mbtag():
    connection = engine.connect()
    
    query = 'SELECT COUNT(1) FROM dbo.msd_r_mbtag'
    ResultProxy = connection.execute(query)
    assert ResultProxy.fetchall()[0][0] == 2319
    
    connection.close()
