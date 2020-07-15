import configparser
import os
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col

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
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
    print("Spark Session Created")

def process_data(spark, s3_bucket):
    print ("-> Airport Data Processing STARTED")
  
    # get filepath to airport data file
    csv_data = s3_bucket + 'rawdata/codes/airport_codes.csv'
    df = spark.read.format('csv').load(csv_data, header=True, inferSchema=True)
    
    # create temp airport table to write SQL Queries
    df.createOrReplaceTempView("airport_temp_table")
    df.printSchema()
    
    # extract columns to create airport table
    print ("-->  Airport Data Extraction STARTED")
    airport_table = spark.sql(" SELECT distinct att.ident iata_code,\
                            att.name airport_name, \
                            REPLACE(SUBSTR(att.iso_region,INSTR(att.iso_region,'-'),LENGTH(att.iso_region)),'-','') air_state_code, \
                            att.municipality location, \
                            SUBSTR (att.coordinates,0,INSTR(att.coordinates,',')-1) airport_latitude, \
                            SUBSTR (att.coordinates,INSTR(att.coordinates,',')+1,LENGTH(att.coordinates)) airport_longitude, \
                            elevation_ft \
                            FROM airport_temp_table att\
                            WHERE att.iso_country = 'US' and att.iso_country IS NOT NULL")
    print ("-->  Airport Data Extraction COMPLETED")
    
    # write airport data in parquet format in S3 location   
    print ("--->  Airport Data Parquet Writing STARTED")
    airport_table.write.mode('overwrite').parquet(s3_bucket + 'datalake/airport_table/')
    print ("--->  Airport Data Parquet Writing COMPLETED")
    
    print ("-> Airport Data Processing COMPLETED")
    
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
    
