from msd.download_torrent_on_mac import download_torrent_on_mac
from msd.migrate_to_mysql import migrate_to_mysql

class Pipeline():
    def download_torrent_on_mac(self, download_url, target_folder):
        download_torrent_on_mac(download_url=download_url, target_folder=target_folder)

    def migrate_to_mysql(self, mysql_server, mysql_username, mysql_password, mysql_port, mysql_db):
        migrate_to_mysql(mysql_server=mysql_server, mysql_username=mysql_username, mysql_password=mysql_password,
                         mysql_port=mysql_port, mysql_db=mysql_db)

msd_pipeline = Pipeline()

msd_pipeline.download_torrent_on_mac(
    download_url='https://academictorrents.com/download/e0b6b5ff012fcda7c4a14e4991d8848a6a2bf52b.torrent',
    target_folder='/Users/derek-funk/Documents/data-engineering/springboard-derek-funk/capstone/msd'
)

msd_pipeline.migrate_to_mysql(
    mysql_server='localhost',
    mysql_username='root',
    mysql_password=<mysql_password>,
    mysql_port=3306,
    mysql_db='msd'
)
