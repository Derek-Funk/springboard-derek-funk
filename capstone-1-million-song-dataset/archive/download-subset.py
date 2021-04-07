# This file will download this data set (3 GB) using a bittorrent client:
# https://academictorrents.com/download/e0b6b5ff012fcda7c4a14e4991d8848a6a2bf52b.torrent
# Step through this file line by line instead of running all at once.
# This example can be generalized for any kind of torrent download.

# setup
import os
import tarfile
download_path = '/Users/derek-funk/Downloads/'
project_path = '/Users/derek-funk/Documents/data-engineering/springboard-derek-funk/capstone'

# install Homebrew and wget
# source: https://brew.sh/
os.system('/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"')
os.system('brew install wget')

# install transmission (bittorrent) client and use it to unpack torrent file
# source: https://cli-ck.io/transmission-cli-user-guide/
os.system('brew install transmission')

# starts transmission session
os.system('transmission-daemon')

# downloads torrent into Downloads folder, should respond with "success" even before it's 100% done downloading
download_link = 'https://academictorrents.com/download/e0b6b5ff012fcda7c4a14e4991d8848a6a2bf52b.torrent'
os.system(f'transmission-remote -a {download_link}')

# check status of download (this download takes about ~15 min)
os.system('transmission-remote -l')

# after download status says 100% done, stop transmission session (you need to look up torrent id from download status)
torrent_id = 3
os.system(f'transmission-remote -t {torrent_id} -r')

# move from Downloads folder to project folder
tar_gz_file_name = 'millionsongsubset_full.tar.gz'
os.system(f' mv {download_path}{tar_gz_file_name} {project_path}')

# unzip file
# source: https://stackoverflow.com/questions/30887979/i-want-to-create-a-script-for-unzip-tar-gz-file-via-python
with tarfile.open(tar_gz_file_name, 'r:gz') as file:
    file.extractall()

# remove zip file
os.system(f'rm {tar_gz_file_name}')