# 1 - Background

## 1.1 - Objective
The second capstone in Springboard's Data Engineering bootcamp was a "guided" capstone. The problem statement was provided and the following technologies were used: Azure Blob Storage, Azure Databricks, PySpark DataFrames & SQL, Parquet files.

## 1.2 - Topic
This project involves an ETL that takes NASDAQ and NYSE data from Azure Blob Storage in the form of parquet files, transforms them in Azure Databricks, then writes the final output back to Blob Storage. The data are stock price transactions, most of which are quoted prices and some of which are actual traded prices. The purpose of the ETL is to transform the data in a way that results in just the quotes, with the most recent trades and yesterday's closing trade as extra columns. The following sections will make this more clear.

## 1.3 - Datasets
All input data can be found in [data](https://github.com/Derek-Funk/springboard-derek-funk/tree/master/capstone-2-adb-etl/data)
