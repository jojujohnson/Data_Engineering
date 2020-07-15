import configparser
import os
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *

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

def process_data(spark, s3_bucket):
    print ("-> Data Processing STARTED")
    
    print ("-->  Demographics Data Extraction STARTED")
    
    #Load demographics table
    us_demographics = spark.read.format('csv').load(s3_bucket + 'rawdata/demographics/us-cities-demographics.csv', header=True, inferSchema=True, sep=';')\
                                .withColumn("male_population", col("Male Population").cast(LongType()))\
                                .withColumn("female_population", col("Female Population").cast(LongType()))\
                                .withColumn("total_population", col("Total Population").cast(LongType()))\
                                .withColumn("num_veterans", col("Number of Veterans").cast(LongType()))\
                                .withColumn("foreign_born", col("Foreign-born").cast(LongType()))\
                                .withColumnRenamed("Average Household Size", "avg_household_size")\
                                .withColumnRenamed("State Code", "state_code_d")\
                                .withColumnRenamed("Race","race")\
                                .withColumnRenamed("Median Age", "median_age")\
                                .withColumnRenamed("City", "city_d")\
                                .drop("State", "Count", "Male Population", "Female Population", 
                                      "Total Population", "Number of Veterans", "Foreign-born")
    
    #
    us_city = spark.read.parquet(s3_bucket + "datalake/city_state_table/")
    
    #
    demographics_city_table = us_demographics.join(us_city, (us_demographics.city_d == us_city.city) & (us_demographics.state_code_d == us_city.state_code), "inner")\
                                              .drop ("city" , "state_code")
    
    #
    print ("-->  Demographics Data Extraction COMPLETED")
    
    print ("--->  Demographics Data Parquet Writing STARTED")
    demographics_city_table.write.mode('overwrite').parquet(s3_bucket + 'datalake/demographics_city_table/')
    print ("--->  Demographics Data Parquet Writing COMPLETED")
    
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