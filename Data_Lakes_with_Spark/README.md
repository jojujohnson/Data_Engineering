Project : DataLake
==================

Introduction
------------

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Project Structure
-----------------

Following files are part of the project performs respective purpose:-

    dl.cfg: Contains reference for AWS credentials (KEY and SECRET)

    etl.py: Program Extracts json formated songs and log data from S3, Transforms data using Spark, and Loads the dimensional and fact tables created in parquet format back to S3.

    README.md: contains detailed information about the project.

    S3 Load: Screen shot of tables created and stored in S3 after execution completion

Project Summary
-----------------

Source Data which is in JSON format available in AWS S3 read , tranformed and processed using spark and finally load back to S3.
S3, EC2 and EMR instances are used for storage and processing capabilites.
Fact and Dimension tables are created and stored in S3.
EMR instance is terminated upon completion.

Script execution
----------------

1. Create AWS Account. setup S3 , EC2 and EMR instances .

2. Add KEY and SECRET to dl.cfg
   KEY=YOUR_AWS_ACCESS_KEY
   SECRET=YOUR_AWS_SECRET_KEY

3. Login to EMR instance.

4. Copy etl.py script and dl.cfg config files to home directory /home/hadoop

5. Execute etl.py script for performing required processing steps.
	Command : spark-submit etl.py

6. Wait for execution completion

7. Validate results in Storage S3 bucket.

