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
        .config("spark.sql.broadcastTimeout", "36000")\
        .getOrCreate()
    return spark

def process_data(spark, s3_bucket):
    """
       Process various data sets using spark 
    """    
    print ("-> Immigration Demographic Data Processing STARTED")
    
    print ("--> Immigration Demographic Data Extraction STARTED")
    
    immigrant = spark.read.parquet(s3_bucket + "datalake/immigrant_table/")
    immigration = spark.read.parquet(s3_bucket + "datalake/immigration_table/")
    city_state = spark.read.parquet(s3_bucket + "datalake/city_state_table/")
    country = spark.read.parquet(s3_bucket + "datalake/country_table/")
    demographics_city_table = spark.read.parquet(s3_bucket + "datalake/demographics_city_table/")
    
    print("demographics_city_temp_table")
    demographics_city_table.createOrReplaceTempView("demographics_city_temp_table")
    demographics_city_table.printSchema()
    
    print("immigrant_temp_table")
    immigrant.createOrReplaceTempView("immigrant_temp_table")
    immigrant.printSchema()
    
    print("immigration_temp_table")
    immigration.createOrReplaceTempView("immigration_temp_table")
    immigration.printSchema()
    
    print("country_temp_table")
    city_state.createOrReplaceTempView("country_temp_table")
    city_state.printSchema()
    
    print("city_state_temp_table")
    country.createOrReplaceTempView("city_state_temp_table")
    country.printSchema()
    
    print ("agg_demographics_city_table")
    agg_demographics_city_table = spark.sql("SELECT city_d as city, state, state_code_d as state_code,\
                                    mean(median_age) avg_age, mean(total_population) avg_total_population, mean(foreign_born) avg_foreign_born\
                                    from demographics_city_temp_table dctt\
                                    group by city , state , state_code")
    
    agg_demographics_city_table.createOrReplaceTempView("agg_demographics_city_temp_table")
    agg_demographics_city_table.printSchema()
    
    print ("immigration_table")
    immigration_table = spark.sql(" SELECT ctt.state_code state_code, sum(from_country_code) total_country, sum(iata_code) total_airport ,\
                                    sum(airline) total_airline, sum(visa_type) total_visa_type \
                                    from immigrant_temp_table itt\
                                    INNER JOIN \
                                    immigration_temp_table ctt\
                                    on itt.cicid=ctt.cicid \
                                    group by state_code")

    immigration_table.createOrReplaceTempView("immigration_temp_table")
    immigration_table.printSchema()

    
    print ("immigration_demographics_table")
    immigration_demographics_table = spark.sql("SELECT adctt.state_code , total_country, total_airport, total_airline, total_visa_type, \
                                               avg_age , avg_total_population, avg_foreign_born \
                                               from agg_demographics_city_temp_table adctt\
                                               INNER JOIN \
                                               immigration_temp_table itt\
                                               on adctt.state_code=itt.state_code")
        
    # write  Immigration Demographic data in parquet format in S3 location
    print ("--> Immigration Demographic Data Extraction COMPLETED")
    
    print ("---> Immigration Demographic Data Parquet Writing STARTED")
    immigration_demographics_table.write.mode("overwrite").parquet(s3_bucket + 'datalake/immigration_demographics_table/')
    print ("---> Immigration Demographic Data Parquet Writing COMPLETED")
    
    print ("-> Immigration Demographic Data Processing COMPLETED")

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
    