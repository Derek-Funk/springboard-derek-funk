import os
import pathlib
import subprocess
import tarfile
import time

def download_torrent_on_mac(download_url, target_folder):
    t0 = time.time()

    print('\n' + '-' * 100, 'STAGE 1: DOWNLOADING TORRENT', '-' * 100 + '\n', sep='\n')

    if os.system('which brew') != 0:
        print('\n' + '-' * 100, 'For macOS, package Homebrew must be installed.',
              'As administrator, run the following in the terminal:',
              '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"',
              '-' * 100 + '\n', sep='\n')
        return

    if os.system('brew list wget') != 0:
        print('\n' + '-' * 100, 'Downloading package wget...', '-' * 100 + '\n', sep='\n')
        os.system('brew install wget')

    if os.system('brew list transmission') != 0:
        print('\n' + '-' * 100, 'Downloading package transmission...', '-' * 100 + '\n', sep='\n')
        os.system('brew install transmission')

    print('\n' + '-' * 100, 'Starting BitTorrent download session...', '-' * 100 + '\n', sep='\n')
    os.system('transmission-daemon')
    os.system(f'transmission-remote -a {download_url}')
    download_ongoing = True
    t1 = time.time()
    while download_ongoing:
        tx = time.time()
        download_time = round((tx - t1) / 60)
        download_progress = subprocess.Popen("transmission-remote -l | grep millionsongsubset | awk '{ print $2 }'",
                                             shell=True, stdout=subprocess.PIPE, ).communicate()[0].decode('utf-8').strip('\n')
        print('\n' + '-' * 100, f'Download {download_progress} complete. Status will be checked every 5 minutes.',
              f'Download time so far: {download_time} minutes.' ,'-' * 100 + '\n', sep='\n')
        time.sleep(60 * 5)
        download_status = subprocess.Popen("transmission-remote -l | grep millionsongsubset | awk '{ print $5 }'",
                                           shell=True, stdout=subprocess.PIPE, ).communicate()[0].decode('utf-8').strip('\n')
        if download_status == 'Done':
            download_ongoing = False
            t2 = time.time()
            download_time = round((t2 - t1) / 60)
            print('\n' + '-' * 100, 'Download 100% complete.',
                  f'Download time: {download_time} minutes.', '-' * 100 + '\n', sep='\n')
            torrent_id = int(subprocess.Popen("transmission-remote -l | grep millionsongsubset | awk '{ print $1 }'", shell=True,
                             stdout=subprocess.PIPE, ).communicate()[0].decode('utf-8').strip('\n'))

    print('\n' + '-' * 100, 'Cleanup: moving download to target_folder, unzipping, and removing old zipped download...', '-' * 100 + '\n', sep='\n')
    path_to_downloads_folder = str(pathlib.Path.home() / 'Downloads')
    download_name = subprocess.Popen("transmission-remote -l | grep millionsongsubset | awk '{ print $10 }'",
                                     shell=True, stdout=subprocess.PIPE, ).communicate()[0].decode('utf-8').strip('\n')
    path_to_download = path_to_downloads_folder + '/' + download_name
    os.system(f'transmission-remote -t {torrent_id} -r')
    with tarfile.open(path_to_download, 'r:gz') as file:
        file.extractall(target_folder)
    os.system(f'rm {path_to_download}')

    t3 = time.time()
    download_time = round((t3 - t0) / 60)
    print('\n' + '-' * 100, 'END OF STAGE 1: DOWNLOADING TORRENT', f'Stage 1 duration: {download_time} minutes.', '-' * 100 + '\n', sep='\n')

# download_torrent_on_mac(
#     download_url='https://academictorrents.com/download/e0b6b5ff012fcda7c4a14e4991d8848a6a2bf52b.torrent',
#     target_folder='/Users/derek-funk/Documents/data-engineering/springboard-derek-funk/capstone/msd'
# )