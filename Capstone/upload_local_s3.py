import boto3
import configparser
import os

#
config = configparser.ConfigParser()
config.read('/home/workspace/dwh.cfg')
os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS_CREDENTIALS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS_CREDENTIALS", "AWS_SECRET_ACCESS_KEY")

#
bucket_name = 'capstone-data-engineering'
folder='rawdata'
    
s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
for obj in bucket.objects.filter(Prefix=folder+'/SAS/'):
    s3.Object(bucket.name,obj.key).delete()
    
s3 = boto3.client('s3')

print(os.environ['HOME'])
os.chdir(os.environ['HOME'])

if os.path.exists(os.environ['HOME'] +'/SASFile'):
    print ("File Path Exist")
else:    
    os.mkdir("SASFile")

root = "../../data/18-83510-I94-Data-2016/"
#root = "/home/workspace/data/data/"
files = [root+f for f in os.listdir(root)]
for f in files:
    print ("File Processing : ",f)
    s3.upload_file(f, bucket_name , folder + "/i94_immigration_data/" + f.split("/")[-1])
    s3.download_file(bucket_name, folder + "/i94_immigration_data/" + f.split("/")[-1], os.environ['HOME'] +'/SASFile/'+ f.split("/")[-1])

    
s3.upload_file("/home/workspace/data/airport_codes.csv", bucket_name , folder + "/codes/airport_codes.csv")

s3.upload_file("/home/workspace/data/state_code.csv", bucket_name , folder +  "/codes/state_code.csv")

s3.upload_file("/home/workspace/data/country_code.csv",  bucket_name , folder + "/codes/country_code.csv")

s3.upload_file("/home/workspace/data/us-cities-demographics.csv", bucket_name , folder + "/demographics/us-cities-demographics.csv")

s3.upload_file("../../data2/GlobalLandTemperaturesByCity.csv", bucket_name , folder + "/temperatures/GlobalLandTemperaturesByCity.csv")
 

    
    
  

