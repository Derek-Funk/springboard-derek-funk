# 2 - Local Pipeline with BitTorrent and MySQL

## 2.1 - Purpose
The purpose of this pipeline was to get an end-to-end process from the MSD subset to a local database, which in my case was MySQL. It also
helped in figuring out:
1. What the final Entity Relationship Diagram (ERD) of the database would be
2. How to use the Python package SQLAlchemy to connect to a database and create/populate the database



## Files
* download_torrent_on_mac.py - this module can be used to download a torrent via BitTorrent technology
* migrate_to_mysql.py - this module can be used to clean the million song dataset and migrate to a local MySQL server
* master.py - this file can be used to run the above 2 in order to complete the download and migration in one run

## Directions to run
The master.py shows how everything was run on my local machine. All the inputs in this file can be replaced with another Mac user's to run this pipeline on their machine.

## Runtime estimates
* download_torrent_on_mac.py - this portion takes on average 45 minutes, though can fluctuate a lot
* migrate_to_mysql.py - this portion takes around 35 minutes
