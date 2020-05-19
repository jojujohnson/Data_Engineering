Project : Data Modeling With Postgres
==================

Introduction
------------

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app


Project Structure
-----------------

Following files are part of the project performs respective functionalities:-

    Data : Folder contains the JSON data.
    
    sql_queries.py: Contains SQL queries to DROP tables , CREATE tables , LOAD data to tables , SELECT statements.
    
    create_tables.py : Script to DROP and CREATE Dimenion and Fact tables. Can be used to reset tables. 
    
    etl.py: Program Extracts json formated songs and log data load to Dimension and Fact tables.
        
    README.md:  Contains detailed information about the project.

    etl.ipynb: Notebook to perform etl functionalities. Same details are used in etl.py script.
    
    test.ipynb: Notebook to test connectivity and displays the first few rows of each table to let you check your database
    
    
    
Script execution
----------------

1. create DROP, CREATE and INSERT query statements and add to script sql_queries.py

2. Run in terminal :  python create_tables.py
    create_tables.py script connects to sparkifydb, Drop tables if exists and create new ones based on specification. Connection is closed after operation completion.

3. Run in terminal :  python etl.py
    etl.py scripts connects to sparkifydb . Songs and Logs JSON data is iterated and inserted to specified tables.
    Connection is closed after operation
    
4. Run test.ipynb notebook to verify successfull data load.    

Notes
-----

Source Data
-----------
Song Dataset
song_data/A/B/C/TRABCEI128F424C983.json

Songs
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

Logs
log_data/2018/11/2018-11-12-events.json
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}


