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
    print("Spark Session Created")

    
def process_data(spark, s3_bucket):
    """
       Process various data sets using spark 
    """
    print ("-> Data Processing STARTED")
    
    # read CSV data file     
    df_spark = spark.read.format('csv').load(s3_bucket + 'rawdata/temperatures/GlobalLandTemperaturesByCity.csv', header=True, inferSchema=True)\
        
    # create temp immigration table to write SQL Queries
    df_spark.createOrReplaceTempView("weather_temp_table")
    df_spark.printSchema()
    
    # extract columns to create airport table
    print ("-->  Data Extraction STARTED")
    weather_table = spark.sql(" SELECT distinct City as city, \
                                Latitude as city_latitude,\
                                Longitude as city_longitude,\
                                avg(AverageTemperature) as avg_temp, \
                                avg(AverageTemperatureUncertainty) as std_temp \
                                FROM weather_temp_table wtt \
                                where Country = 'United States' \
                                group by city , city_latitude , city_longitude")
    
    print ("-->  Data Extraction COMPLETED")
    
    # write weather data in parquet format in S3 location   
    print ("--->  Data Parquet Writing STARTED")
    weather_table.write.mode('overwrite').parquet(s3_bucket + 'datalake/weather_table/')
    print ("--->  Data Parquet Writing COMPLETED")

    print ("-->  Data Extraction STARTED")
    us_city_state = spark.read.parquet(s3_bucket + "datalake/city_state_table/")
    city_weather_table = weather_table.join(us_city_state, "city", "inner").drop("city_latitude", "city_longitude")
    print ("-->  Data Extraction COMPLETED")
    
    # write cities with temperature data in parquet format in S3 location
    print ("--->  Data Parquet Writing STARTED")
    city_weather_table.write.mode("overwrite").parquet(s3_bucket + "datalake/city_weather_table")
    print ("--->  Data Parquet Writing COMPLETED")
    
    print ("-> Data Processing COMPLETED")
    
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
    


