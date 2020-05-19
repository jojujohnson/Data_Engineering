import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('/home/hadoop/dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
"""
   Create spark session for processing
"""
        
    print("Create Spark Session")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
"""
   Function to read, process and store songs and artist data using Songs Data.
"""    
    print ("-> Song Data Files Processing STARTED")
    
    # get filepath to song data file
    songs_data = input_data + 'song_data/*/*/*/*.json'
    
    # read log data file
    df = spark.read.json(songs_data)
    
    # create temp song table to write SQL Queries
    df.createOrReplaceTempView("songs_temp_table")
    df.printSchema()
    
    # extract columns to create songs table
    print ("--> Songs Data Extraction STARTED")
    songs_table = spark.sql(" SELECT distinct stt.song_id,\
                            stt.title,\
                            stt.artist_id,\
                            stt.year,\
                            stt.duration\
                            FROM songs_temp_table stt\
                            WHERE stt.song_id IS NOT NULL")
    print ("--> Songs Data Extraction COMPLETED")
     
    # write songs data in parquet format partioned by year and artist_id in S3 location   
    print ("---> Songs Data Parquet Writing STARTED")
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')
    print ("---> Songs Data Parquet Writing COMPLETED")   
    
    # extract columns to create songs table
    print ("----> Artists Data Extraction STARTED")
    artists_table = spark.sql(" SELECT DISTINCT att.artist_id,\
                                att.artist_name,\
                                att.artist_location,\
                                att.artist_latitude,\
                                att.artist_longitude\
                                FROM songs_temp_table att\
                                WHERE att.artist_id IS NOT NULL")
    print ("----> Artists Data Extraction COMPLETED")
    
    # write artist data in parquet format in S3 location  
    print ("-----> Artists Data Parquet Writing STARTED")
    songs_table.write.mode('overwrite').parquet(output_data+'artists_table/')
    print ("-----> Artists Data Parquet Writing COMPLETED")
        
    print ("-> Song Data Files Processing COMPLETED")
    
def process_log_data(spark, input_data, output_data):
"""
   Function to read, process and store users, time and songs_play data using Logs Data.
"""     
    print ("-> Log Data Files Processing STARTED")
    
    # Define filepath to log data file
    log_path = input_data + 'log_data/*/*/*.json'
    
    # read log data files
    df = spark.read.json(log_path)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # create temp log table to write SQL Queries
    df.createOrReplaceTempView("log_temp_table")
    df.printSchema()
    
    # extract columns for users table
    print ("--> Users Data Extraction STARTED")
    users_table = spark.sql("SELECT distinct ltt.userId as user_id,\
                             ltt.firstName as first_name,\
                             ltt.lastName as last_name,\
                             ltt.gender as gender,\
                             ltt.level as level \
                             FROM log_temp_table ltt\
                             WHERE ltt.userId IS NOT NULL")
    print ("--> Users Data Extraction COMPLETED")
    
    # write users table to parquet files
    print ("---> Users Data Parquet Writing STARTED")
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')
    print ("---> Users Data Parquet Writing COMPLETED")
    
    # extract columns to create time table
    print ("----> Time Data Extraction STARTED")
    time_table = spark.sql("SELECT \
                            A.start_time_sub as start_time,\
                            hour(A.start_time_sub) as hour,\
                            dayofmonth(A.start_time_sub) as day,\
                            weekofyear(A.start_time_sub) as week,\
                            month(A.start_time_sub) as month,\
                            year(A.start_time_sub) as year,\
                            dayofweek(A.start_time_sub) as weekday \
                            FROM \
                            (SELECT to_timestamp(time_ltt.ts/1000) as start_time_sub \
                            FROM log_temp_table time_ltt \
                            WHERE time_ltt.ts IS NOT NULL \
                            ) A")
    print ("---> Time Data Extraction COMPLETED")
    
    # write time table to parquet files partitioned by year and month
    print ("----> Time Data Parquet Writing STARTED")
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')
    print ("----> Time Data Parquet Writing COMPLETED")

    # Define filepath to Song data files
    songs_data = input_data + 'song_data/*/*/*/*.json'
    
    # read songs data files
    df = spark.read.json(songs_data)
    
    # create temp songs table to write SQL Queries
    df.createOrReplaceTempView("songs_temp_table")
    df.printSchema()

    # Join song and log datasets to create songplays fact table 
    print ("-----> SongPlays Data Extraction STARTED")
    songplays_table = spark.sql(" SELECT monotonically_increasing_id() as songplay_id,\
                                to_timestamp(logT.ts/1000) as start_time,\
                                month(to_timestamp(logT.ts/1000)) as month,\
                                year(to_timestamp(logT.ts/1000)) as year,\
                                logT.userId as user_id,\
                                logT.level as level,\
                                songT.song_id as song_id,\
                                songT.artist_id as artist_id,\
                                logT.sessionId as session_id,\
                                logT.location as location,\
                                logT.userAgent as user_agent\
                                FROM log_temp_table logT\
                                JOIN songs_temp_table songT on logT.artist = songT.artist_name and logT.song = songT.title")
    print ("-----> SongPlays Data Extraction COMPLETED")

    # write songplays data in parquet format partitioned by year and month in S3 location 
    print ("-----> Songplays Data Parquet Writing STARTED")
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')  
    print ("-----> Songplays Data Parquet Writing COMPLETED")

    print ("-> Logs Data Files Processing COMPLETED")
    
    
def main():
    """
        Main Function to perform extract data from S3, transform all data in JSON format and load back to S3 using spark.
    """    
    spark = create_spark_session()
    
    #Declare Inout and Output S3 bucket locations
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkifydb/"
    
    #Invoke Functions to process data
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    
if __name__ == "__main__":
    main()