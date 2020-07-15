Project : DataLake
==================

Introduction
------------

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow. Sparkify has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


Project Structure
-----------------

Following files are part of the project performs respective purpose:-

    Following files are available under Path : /home/workspace/airflow/
    	README.md: Contains detailed information about the project.
        
    	dwh.cfg: Contains reference for AWS credentials (ACCESS KEY and SECRET ACCESS KEY)

    	conn_start.py: Script to establish the connection to AWS redshift and create DB cluster.

    	conn_close.py: Script to delete cluster and close connection
   
    	create_tables.py: contains SQL statements to perform DROP and CREATE table SQL operations
    
    	create_db_tables.py: Script to execute DROP and CREATE DB tables statements. Can be used to reset DB level objects. 
        
        Airflow Connection.png : Screen shot of configured Connections.
    
    Following files are available under Path : /home/workspace/airflow/plugins/helpers
    	sql_queries.py: contains SQL statements to perform select and load SQL operations. 
    
    Following files are available under Path : /home/workspace/airflow/dags
	    udac_example_dag.py: Script which performs the data load functionality. Moves data from S3 to Redshift. Logging is enabled.  
    
    Following files are available under Path : /home/workspace/airflow/plugins/operators
       	stage_redshift.py: Stage Data from S3 to Redshift staging tables
    	load_fact.py: Load data from Staging tables to Fact table
        load_dimension.py: Load data from Fact table to Dimension tables
    	data_quality.py: Check if all loaded tables have records. Fail if Record Count less than 1.
    
    
Project Summary
-----------------

Source Data which is in JSON format available in AWS S3 is loaded to AWS Redshift with appropriate connections setup.
From creation of cluster to Data load to Dimension and Fact tables and finally validating the record count is executed through python scripts.
Cluster creation once started waits automatically untill cluster status becomes available.
Upon creation of cluster , table creation and data load is followed and finally validated.
Connection to cluster is closed and cluster is deleted at end.


Script execution
----------------

1. Start the airflow services
	Command : /opt/airflow/start.sh

2. click on Access Airflow button and enable Airflow UI

3. setup NEW coneection : Got to Airflow UI -> Admins -> Connections
	a. aws_credentials
	b. redshift 

4. Configure AWS credentials and connections in dwh.cfg file.

5. Execute Conn_start.py script to build a NEW cluster in Redshift. Conn_start.py checks every 1 minute until New cluster is made available.
	Command : python /home/workspace/airflow/conn_start.py

6. Execute create_db_tables.py to DROP and CREATE tables in New Redshift cluster. create_db_tables.py internally calls create_tables.py to perform the functionality.
	Command : python /home/workspace/airflow/create_db_tables.py

7. Execute udac_example_dag.py script to copy data from S3 bucket to Fact and Dimension Tables.
    Airflow Dependenices are defined in script udac_example_dag.py 

8. Validate in job execution logs to confirm records are loaded at DB level tables. 

9. Execute conn_close.py script to close cluster connection and finally delete cluster after usage.
	Command : python /home/workspace/airflow/conn_close.py


