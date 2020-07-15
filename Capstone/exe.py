import os 
print ("Script Execution Started: upload_local_s3.py")
os.system('python /home/workspace/upload_local_s3.py')
print ("Script Execution Completed : upload_local_s3.py")

print ("Script Execution Started: us_airport.py")
os.system('python /home/workspace/Data_Build_Load/us_airport.py') 
print ("Script Execution Completed : us_airport.py")

print ("Script Execution Started: us_city_state.py")
os.system('python /home/workspace/Data_Build_Load/us_city_state.py')
print ("Script Execution Completed : us_city_state.py")

print ("Script Execution Started: us_immigration.py")
os.system('python /home/workspace/Data_Build_Load/us_immigration.py')  
print ("Script Execution Completed : us_immigration.py")

print ("Script Execution Started: us_weather_city.py")
os.system('python /home/workspace/Data_Build_Load/us_weather_city.py') 
print ("Script Execution Completed : us_weather_city.py")

print ("Script Execution Started: us_demographics_city.py")
os.system('python /home/workspace/Data_Build_Load/us_demographics_city.py') 
print ("Script Execution Completed : us_demographics_city.py")

print ("Script Execution Started: us_immigration_demographics.py")
os.system('python /home/workspace/Data_Build_Load/us_immigration_demographics.py') 
print ("Script Execution Completed : us_immigration_demographics.py")

print ("Script Execution Started: us_airport_weather.py")
os.system('python /home/workspace/Data_Build_Load/us_airport_weather.py')     
print ("Script Execution Completed : us_airport_weather.py")

print ("Script Execution Started: table_quality_check.py")
os.system('python /home/workspace/Data_Build_Load/table_quality_check.py')
print ("Script Execution Completed : table_quality_check.py")

print ("Script Execution Started: analytics_quality_check.py")
os.system('python /home/workspace/Data_Build_Load/analytics_quality_check.py') 
print ("Script Execution Completed : analytics_quality_check.py")




