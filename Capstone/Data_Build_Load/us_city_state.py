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

def process_data(spark, s3_bucket):
    print ("-> City State Data Processing STARTED")
    
    print ("--> City State Data Extraction STARTED")
    
    #Load country table
    spark.read.format('csv').load(s3_bucket + 'rawdata/codes/country_code.csv', header=True, inferSchema=True)\
                        .write.mode("overwrite").parquet(s3_bucket + "datalake/country_table/")
    
    #Read Demographics Data
    us_demographics = spark.read.format('csv').load(s3_bucket + 'rawdata/demographics/us-cities-demographics.csv', header=True,   inferSchema=True, sep=';')\
                .select("City","State","State Code")\
                .withColumnRenamed("City", "city")\
                .withColumnRenamed("State", "state")\
                .withColumnRenamed("State Code", "state_code")
    
    print ("demographics")
    us_demographics.printSchema()
    
    #Read Airport Data
    us_airport = spark.read.format('csv').load(s3_bucket + 'rawdata/codes/airport_codes.csv', header=True, inferSchema=True)\
                 .filter("iso_country = 'US'")\
                 .select("iso_region", "municipality")\
                 .withColumnRenamed("iso_region".strip().split('-')[-1], "state_code")\
                 .withColumnRenamed("municipality", "city")
    
    print ("airport")
    us_airport.printSchema()
    
    #Read State Code Data
    us_states = spark.read.format('csv').load(s3_bucket + 'rawdata/codes/state_code.csv', header=True, inferSchema=True)\
                .withColumnRenamed("State_Code", "j_state_code")
    
    print ("states")
    us_states.printSchema()
     
    #Join Airpot with States Data  
    us_airport = us_airport.join(us_states, (us_airport.state_code==us_states.j_state_code), "inner")\
                           .select("city","state","state_code")\
                           .drop("j_state_code")
    
    print ("airport")
    us_airport.printSchema()
    
    #Merge airport and demographics data
    us_city_state = us_airport.union(us_demographics)\
                 .drop_duplicates()

    #
    print ("--> City State Data Extraction COMPLETED")
    
    print ("---> City State Data Parquet Writing STARTED")
    us_city_state.write.mode("overwrite").parquet(s3_bucket + 'datalake/city_state_table/')
    print ("---> City State Data Parquet Writing COMPLETED")
    
    print ("-> City State Data Processing COMPLETED")

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