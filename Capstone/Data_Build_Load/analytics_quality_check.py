import boto3
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

def check(path, table,spark,SQL):
    
    print ("======================================")
    checkvar=path + table
    print("Check Activated : " , checkvar)
    print("SQL Query : " , SQL)
    
    temp_table = spark.read.parquet(checkvar)
    temp_table.createOrReplaceTempView("temp_table")
    
    temp_table = spark.sql(SQL).first()
    print(table ," count :",temp_table[0])
    if (temp_table[0] > 0):
            print ("PASSED")
    else:
            print ("FAILED")
    
    print ("======================================")
    print ("")
    
def create_spark_session():
    """
       Create spark session for processing
    """
    print("Create Spark Session")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def main():
    """
        Main Function to load data to S3 using spark.
    """    
    
    #Print S3 bucket location
    s3_bucket=os.environ["s3_bucket"]
    s3_bucket = s3_bucket.replace("'", "")
 
    print (s3_bucket)

    spark = create_spark_session()
    print("Spark Session Created")

    #Invoke Functions to check data  
    check(s3_bucket + "datalake/", "country_table",spark,"SELECT count(code_2digit) total_country FROM temp_table")
    check(s3_bucket + "datalake/", "airport_table",spark,"SELECT count(iata_code) total_airport FROM temp_table")


if __name__ == "__main__":
    main()
