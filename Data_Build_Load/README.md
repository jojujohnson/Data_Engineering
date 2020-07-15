
Project : Data Engineering Capstone
===================================

Introduction
------------
- As more and more immigrants move to the US, people want quick and reliable ways to access certain information that can help inform their immigration, such as weather of the destination, demographics of destination. And for regulators to keep track of immigrants and their immigration meta data such as visa type, visa expire date, entry method to the US.

- Using the available data sources listed above, we build a Data Lake available on S3 that can be used to query for weather and demographics of popular immigration destinations, which could be useful for both immigrants and regulators. Regulators can also access data about individual immigrants, date of arrival to the US, visa expiry dates and method of entries to improve decision making.

Project Structure
-----------------
    Data Sources
    Following files are available under Path : /home/workspace/data
    > airport_codes.csv
    - Aiport Code Data: [Source](https://datahub.io/core/airport-codes#data). Includes a collection of airport codes and their    
      respective cities, countries around the world.
    
    > immigration_data_sample.csv
    - World temperature Data: This dataset comes from Kaggle [Source](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-
      temperature-data). Includes temperature recordings of cities around the world for a period of time
    
    > us-cities-demographics.csv
    - US City Demographic Data: This dataset comes from OpenSoft [Source](https://public.opendatasoft.com/explore/dataset/us-cities-
      demographics/export/). Includes population formation of US states, like race and gender.
      
    > state_code.csv
    - State code Data: This dataset comes from Kaggle https://www.kaggle.com/koki25ando/state-code
    
    > country_code.csv
    - Country code Data: This dataset comes from Kaggle https://www.kaggle.com/leighplt/country-code
    
    Following files are available under Path : /data/18-83510-I94-Data-2016/ 
    > I94*.sas7bdat
    - I94 Immigration Data: This data comes from the US National Tourism and Trade Office [Source]
      (https://travel.trade.gov/research/reports/i94/historical/2016.html). This data records immigration records partitioned by month of every year.
    
    Following files are available under Path : /home/workspace/
        README.md: Contains detailed information about the project.
        
        dwh.cfg: Contains reference for AWS credentials (ACCESS KEY and SECRET ACCESS KEY)
        
        upload_local_s3.py :  script to load rawdata set to AWS s3 bucket.
    
    Following files are available under Path : /home/workspace/Data_Build_Load
        us_city_state.py: load city data to s3 in parquet format from airport_codes.csv , us-cities-demographics.csv and state_code.csv stored in S3
        
        us_airport.py: load airport data to s3 in parquet format from airport_codes.csv stored in S3
         
        us_immigration.py: load immigration data to s3 in parquet format from SASfile stored in S3
          
        us_weather_city.py: load weather city data to s3 in parquet format from GlobalLandTemperaturesByCity.csv and city_state_table stored in S3
        
        us_demographics_city.py: load us demographics city data to s3 in parquet format from us-cities-demographics.csv and city_state_table stored in S3
  
        us_immigration_demographics.py: load city data to s3 in parquet format from immigrant_table , immigration_table , city_state_table 
                                        and demographics_city_table stored in S3
  
        us_airport_weather.py: load city data to s3 in parquet format from airport_table and city_weather_table stored in S3
        
        table_quality_check.py: perform check record counts of all tables stored in S3
                
        analytics_quality_check.py: perform analytics of all tables stored in S3
   
Script execution
----------------

python /home/workspace/upload_local_s3.py
python /home/workspace/Data_Build_Load/us_city_state.py
python /home/workspace/Data_Build_Load/us_airport.py
python /home/workspace/Data_Build_Load/us_immigration.py
python /home/workspace/Data_Build_Load/us_weather_city.py
python /home/workspace/Data_Build_Load/us_demographics_city.py
python /home/workspace/Data_Build_Load/us_immigration_demographics.py
python /home/workspace/Data_Build_Load/us_airport_weather.py
python /home/workspace/Data_Build_Load/normalize_quality_check.py
python /home/workspace/Data_Build_Load/analytics_quality_check.py

Design
------
Data Lake Star Schema Design

Design decision to use Data Lake on S3 is due to:
- Ease of schema design, rely on schema-on-read
- Flexibility in adding / removing additional data
- Availability to a wide range of users with access to the S3 bucket

Tools Used
----------
> Python Scripting
> AWS S3 for storage
> Apache Spark for data processing.
> AWS EC2 and EMR for hosting and computing capabilites.
> Apache Airflow for Scheduling data pipeline
> AWS Redshift for Data Analytics with Scalability.

Table designs
-------------
1. Normalized city_state_table: built on city code data from raw demographics and  airport data joined with states data to get city, state and state_code
2. Normalized airport_table: built on raw airport data, filtered for US airports
3. Normalized immigrant_table: Information about individual immigrants, like age, gender, occupation, visa type, built on I94 Immigration dataset
4. Normalized immigration_table: Information about immigration information, such as date of arrival, visa expiry date, airport, means of travel
5. Normalized city_weather_table: built on global weather data, filtered for US cities, joined with ``city_state_table`` table to get ``city_id``
6. Normalized demographics_city_table: built on raw demographics data   joined with  ``city_state_table`` table to get ``city_id``
7. Denormalized airports_weather_table: Joining weather data with airport location, to get the respective weather information for each US airports
8. Denormalized immigration_demographic_table: Joining immigration with demographics data, to get the population of places where immigrants go

Scenarios
---------
- Data increase by 100x. 
    - Increase EMR cluster size to handle bigger volume of data
    -  Since solution is on build on of Amazon cloud, that are easily scalable, the only thing we would need to do is increase the number of nodes of the clusters in EMR to hadle more data. Scaling the whole pipeline should not be a problem .
    - Redshift: Analytical database can be used to optimized for aggregation, also good performance for read-heavy workloads

- Pipelines would be run on 7am daily. 
    - DAG retries, or send emails on failures
    - daily intervals with quality checks
    - if checks fail, then send emails to operators, freeze dashboard, look at DAG logs to figure out what went wrong
    - Also running interval of the Airflow DAG could be changed to daily and scheduled to run overnight to make the data available by 7am

- Make it available to 100+ people
    - With Redshift we can make use of the feature \"elastic resize\" that enables us to add or remove nodes in an Amazon Redshift cluster in minutes. This further increases the agility to get better performance and more storage for demanding workloads, and to reduce cost during periods of low demand.
    - Redshift with auto-scaling capabilities can have good read performance
    
   
