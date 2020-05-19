Project : DataLake
==================

Introduction
------------

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


Project Structure
-----------------

Following files are part of the project performs respective purpose:-

    dwh.cfg: Contains reference for AWS credentials (ACCESS KEY and SECRET ACCESS KEY)

    conn_start.py: Script to establish the connection to AWS redshift and create DB cluster.

    conn_close.py: Script to delete cluster and close connection
   
    sql_queries.py: Script perform DROP , CREATE , INSERT and SELECT SQL operations
    
    create_tables.py: Script to DROP and CREATE DB tables. Can be used to reset DB level objects.
    
    etl.py: Script which performs the data load functionality. Moves data from S3 to Redshift.
    
    sql_validate.py : Script to take recount count of loaded Dimesnion and Fact DB tables
   
    README.md: Contains detailed information about the project.
    

Project Summary
-----------------

Source Data which is in JSON format available in AWS S3 is loaded to AWS Redshift with appropriate connections setup.
From creation of cluster to Data load to Dimension and Fact tables and finally validating the record count is executed through python scripts.
Cluster creation once started waits automatically untill cluster status becomes available.
Upon creation of cluster , table creation and data load is followed and finally valided.
Conn to cluster is closed and cluster is deleted at end.

Script execution
----------------

1. Setup AWS account , IAM User and generate credentials for connectivity and update the information in dwh.cfg file.

2. Execute Conn_start.py script to build a NEW cluster in Redshift. Conn_start.py checks every 1 minute if New cluster is made available.

3. Execute create_tables.py to DROP and CREATE tables in 

4. Execute etl.py script to copy data from S3 bucket to Dimension tables and from Dimension tables to Fact tables.

5. Execute sql_validate.py script to check the record count against Dimesnion and Fact tables.

6. Execute conn_close.py script to close cluster connection and finally delete after usage.


