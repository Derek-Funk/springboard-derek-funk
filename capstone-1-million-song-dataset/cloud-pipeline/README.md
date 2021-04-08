# 3 - Cloud Pipeline with AWS and Azure

# 3.1 Purpose of the Cloud Pipeline
The purpose of this pipeline was to create a similar structure as the <em>Local Pipeline</em>, but using an Azure database as the destination.
There are a few major differences with this approach:
1. The source dataset comes from AWS
2. An Azure Virtual Machine (VM) is required to run the Python SQLAlchemy script
3. Azure SQL Database (which is SQL Server) is used instead of MySQL

