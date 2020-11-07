# This file will download this data set (3 GB) using a bittorrent client.
# Step through this file line by line instead of running all at once.
import os
import tarfile

# install Homebrew and wget
# source: https://brew.sh/
os.system('/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"')
os.system('brew install wget')

# CAN WE GET RID OF THIS SECTION
# download torrent file you want
# source: https://academictorrents.com/about.php#downloading
download_link = 'https://academictorrents.com/download/e0b6b5ff012fcda7c4a14e4991d8848a6a2bf52b.torrent'
os.system(f'wget {download_link}')

# install transmission (bittorrent) client and use it to unpack torrent file
# source: https://cli-ck.io/transmission-cli-user-guide/
os.system('brew install transmission')

# starts transmission session
os.system('transmission-daemon')

# downloads torrent into Downloads folder, should respond with "success" even before it's 100% done downloading
download_link = 'https://academictorrents.com/download/e0b6b5ff012fcda7c4a14e4991d8848a6a2bf52b.torrent'
os.system(f'transmission-remote -a {download_link}')

# check status of download
os.system('transmission-remote -l')

# after download status says 100% done, move from Downloads folder
tar_gz_file_name = 'capstone/millionsongsubset_full.tar.gz'
os.system(f'mv {tar_gz_file_name} ../Documents/data-engineering/springboard-derek-funk/capstone')

# unzip file
# source: https://stackoverflow.com/questions/30887979/i-want-to-create-a-script-for-unzip-tar-gz-file-via-python
with tarfile.open(tar_gz_file_name, 'r:gz') as file:
    file.extractall()

# stop transmission session (you need to look up torrent id from download status)
torrent_id = 2
os.system(f'transmission-remote -t {torrent_id} -r')