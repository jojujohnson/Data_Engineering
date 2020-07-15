import configparser
import os
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import sys
import math
from datetime import datetime, timedelta

config = configparser.ConfigParser()
config.read('/home/workspace/dwh.cfg')
os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS_CREDENTIALS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS_CREDENTIALS", "AWS_SECRET_ACCESS_KEY")
os.environ["s3_bucket"] = config.get("S3", "s3_bucket")

def create_spark_session():
    """
       Create spark session for processing
    """
        
    print("Create Spark Session")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport().getOrCreate()
    return spark

def process_data(spark, s3_bucket):
    """
       Process various data sets using spark 
    """
    print ("-> Airport Weather Data Processing STARTED")
    
    print ("-->  Airport Weather Data Extraction STARTED")
    airport = spark.read.parquet(s3_bucket + "datalake/airport_table/")
    city_state = spark.read.parquet(s3_bucket + "datalake/city_state_table/")
    weather = spark.read.parquet(s3_bucket + "datalake/city_weather_table/")

    airport.createOrReplaceTempView("airport_temp_table")
    city_state.createOrReplaceTempView("city_state_temp_table")
    weather.createOrReplaceTempView("weather_temp_table")

    airport_weather = spark.sql(" select \
    location, iata_code, airport_name, elevation_ft, avg_temp , std_temp, wtt.state, wtt.state_code \
    from \
    airport_temp_table att \
    LEFT OUTER JOIN \
    weather_temp_table wtt \
    ON \
    wtt.city = att.location and wtt.state_code = att.air_state_code")
    
    print ("-->  Airport Weather Data Extraction COMPLETED")
    
    # write Airport Weather data in parquet format in S3 location   
    print ("--->  Airport Weather Data Parquet Writing STARTED")
    airport_weather.write.mode('overwrite').parquet(s3_bucket + 'datalake/airport_weather_table/')
    print ("--->  Airport Weather Data Parquet Writing COMPLETED")
    
    print ("-> Airport Weather Data Processing COMPLETED")
         
def main():
    """
        Main Function to load data to S3 using spark.
    """    
    spark = create_spark_session()
    print("Spark Session Created")

    #Print S3 bucket location
    s3_bucket=os.environ["s3_bucket"]
    s3_bucket = s3_bucket.replace("'", "")
 
    print (s3_bucket)
    
    #Invoke Functions to process data
    process_data(spark, s3_bucket)    
    
if __name__ == "__main__":
    main()
    
