# Derek's Data Engineering Projects
This repository holds all projects for Derek's Springboard Data Engineering Bootcamp. For each project, I go through:
1. What was done, what tools were used, and all related code
2. How the project was done, including images/gifs on the architecture
3. How it can be reproduced

# Main Highlights
[capstone-1-million-song-dataset](capstone-1-million-song-dataset) demonstrates a prototype pipeline that processes 300 GB of unstructured data from AWS into a structured Azure SQL database. Tech stack: AWS volumes, AWS EC2, AWS S3, Azure VMs, Azure SQL Database, Linux, Python, SQLAlchemy.

[capstone-2-adb-etl](capstone-2-adb-etl) demonstrates the usage of Spark in Azure Databricks to collect stock exchange data into Azure Blob Storage and provide daily summary metrics. Tech stack: Azure Blob Storage, PySpark, Spark SQL.

# Additional Mini-Projects
1. [airflow-mini-project](airflow-mini-project) creates a workflow that will email you daily stock exchange data using a Docker infrastructure. Tech stack: Airflow, Docker, Linux, Python.

2. [kafka-mini-project](kafka-mini-project) streams thousands of fake credit card transactions per second to a single-broker Kafka architecture with Docker to process as legit or fraudulent. Tech stack: Kafka, Docker, Python.

3. [spark-mini-project](spark-mini-project) and [hadoop-mini-project](hadoop-mini-project) show how to use Spark and Hadoop locally for testing purposes.

4. [End-to-end ELT in Azure](End-to-end ELT in Azure) is a lab to explore the various features of Azure Data Factory.
