THIS README IS NOT YET FINAL...

# 1 - Background

## 1.1 - Objective
The first capstone in Springboard's Data Engineering bootcamp was an "open" capstone. Students could use any combination of technologies
to process any big data source of their choosing.

## 1.2 - Topic
I chose to work with the [Million Song Dataset](http://millionsongdataset.com/), a collection of 1 million songs and related data such
as artists, genres, popularity, etc. My goal was to process all these unstructured data (which were spread across Hierarchical Data Format (HDF) files, SQLite databases, and text files in various directories) into 1 database where all the same data would be in a new form that was structured, logically connected, and easily accessible.

## 1.3 - Datasets
There are 3 variations of the Million Song Dataset, which I will abbreviate as MSD going forward:
1. A subset of the MSD (just 10,000 songs) available via BitTorrent download. I will call this the <em>BitTorrent subset</em>.
2. The same subset as above except available on AWS as an EBS volume. I will call this the <em>AWS subset</em>.
3. The full MSD also available as an AWS EBS volume. I will call this the <em>AWS MSD</em>.

### 1.3.1 - BitTorrent subset
The <em>BitTorrent subset</em> is available on [Academic Torrents](https://academictorrents.com/details/e0b6b5ff012fcda7c4a14e4991d8848a6a2bf52b). This subset of randomly sampled 10,000 songs amounts to about 3 GB (1% of the full MSD). This dataset was used to build a prototype pipeline using a local MySQL database as the destination. Section 2 walks through this local pipeline.

### 1.3.2 - AWS subset
The <em>AWS subset</em> is available as an AWS Elastic Block Store (EBS) Snapshot (link TBD). This subset was used to build a pipeline from AWS
to an Azure SQL database. Section 3 walks through this pipeline.

### 1.3.3 - AWS MSD
The <em>AWS MSD</em> is also available as an AWS EBS Snapshot (link TBD). Due to time and financial constraints, this pipeline was not created. However, an organization with the resources to utilize a more powerful Azure Virtual Machine (VM) could use the same exact pipeline as with the
<em>AWS subset</em>.

# 2 - Local Pipeline with BitTorrent and MySQL
Navigate to [local-pipeline](https://github.com/Derek-Funk/springboard-derek-funk/tree/master/capstone-1-million-song-dataset/local-pipeline)
for more details.

# 3 - Cloud Pipeline with AWS and Azure
Navigate to [cloud-pipeline](https://github.com/Derek-Funk/springboard-derek-funk/tree/master/capstone-1-million-song-dataset/cloud-pipeline)
for more details.
