import boto3
import configparser
import os
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import sys
import math
from datetime import datetime, timedelta
import pandas as pd 

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
        .config("spark.sql.debug.maxToStringFields", 1000)\
        .enableHiveSupport().getOrCreate()
    return spark

def process_data(spark, s3_bucket):
    """
       Process various data sets using spark 
    """     
    print ("-> Immigration Data Processing STARTED")
    
    # read SAS data file
    root = os.environ['HOME']+'/SASFile/'
    
    files = [root+f for f in os.listdir(root)]
    for f in files:
        print (f)
        if os.path.isfile(f): 
            store_spark = spark.read.format('com.github.saurfang.sas.spark').load(f)\
                                .selectExpr('cast(cicid as int) cicid', 'date_add("1960-01-01",arrdate) as arrdate'  , 'i94port AS iata_code', \
                                 'i94addr AS state_code', 'date_add("1960-01-01",depdate) as depdate', 'dtaddto',\
                                 'airline', 'cast(admnum as long) AS admnum', 'fltno', 'entdepa', \
                                 'entdepd', 'entdepu', 'matflag',\
                                 'i94yr','i94mon','i94res','i94bir','i94visa','visapost','occup','visatype','biryear','gender') 
                                 
            store_spark.write.mode('append').parquet(s3_bucket + 'rawdata/SAS/')
                             
    # create temp immigration table to write SQL Queries
    immigration_table = spark.read.parquet(s3_bucket + 'rawdata/SAS/')
    immigration_table.createOrReplaceTempView("immigration_temp_table")
    print("immigration schema")
    immigration_table.printSchema()
     
    print ("-->  Immigration Data Extraction STARTED")
    immigration_table = spark.sql(" SELECT distinct cast(cicid as int) cicid , arrdate, iata_code ,  state_code, depdate , dtaddto , airline , cast(admnum as long) AS admnum , fltno ,  entdepa , entdepd, entdepu, matflag,i94yr,i94mon FROM immigration_temp_table itt")
    
    print ("-->  Immigration Data Extraction COMPLETED")
         
    # write immigration data in parquet format in S3 location   
    print ("--->  Immigration Data Parquet Writing STARTED")
    immigration_table.write.mode('overwrite').partitionBy("i94yr","i94mon").parquet(s3_bucket + 'datalake/immigration_table/')
    print ("--->  Immigration Data Parquet Writing COMPLETED")   
   
    # extract columns to create immigrant table
    print ("-->  Immigrant Data Extraction STARTED")
    immigrant_table = spark.sql(" SELECT distinct cast(itt.cicid as int) AS cicid,\
                            cast(i94res as int) AS from_country_code, \
                            cast(i94bir as int) AS age, \
                            cast(i94visa as int) AS visa_code, \
                            visapost AS visa_post, \
                            occup AS occupation,\
                            visatype AS visa_type, \
                            cast(biryear as int) AS birth_year, \
                            gender, \
                            i94yr,\
                            i94mon \
                            FROM immigration_temp_table itt")

    print ("-->  Immigrant Data Extraction COMPLETED")
    
    # write immigrant data in parquet format in S3 location   
    print ("--->  Immigrant Data Parquet Writing STARTED")
    immigrant_table.write.mode('overwrite').partitionBy("i94yr","i94mon").parquet(s3_bucket + 'datalake/immigrant_table/')
    print ("--->  Immigrant Data Parquet Writing COMPLETED")
    
    print ("->  Immigration Data Processing COMPLETED")
    
def main():
    """
        Main Function to load data to S3 using spark.
    """    
    #Clear SAS Bucket folder
    bucket_name = 'capstone-data-engineering'
    folder='rawdata'
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix='rawdata/SAS/'):
        s3.Object(bucket.name,obj.key).delete()
    
    #Print S3 bucket location
    s3_bucket=os.environ["s3_bucket"]
    s3_bucket = s3_bucket.replace("'", "")
 
    print (s3_bucket)
   
    spark = create_spark_session()
    print("Spark Session Created")

    #Invoke Functions to process data
    process_data(spark, s3_bucket)    
    
if __name__ == "__main__":
    main()
    
