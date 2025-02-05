
#import necessary dependencies
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
import io
from io import StringIO
import configparser



config = configparser.ConfigParser()
config.read('.env')


aws_secret_key = config['AWS']['secret_key']
aws_access_key = config['AWS']['access_key']


# upload raw_data
raw_dubai_data = pd.read_csv("airbnb-listings-bucket/DubaiData.csv")
raw_la_data = pd.read_csv("airbnb-listings-bucket/LAData.csv")
raw_london_data = pd.read_csv("airbnb-listings-bucket/LondonData.csv")
raw_miami_data = pd.read_csv("airbnb-listings-bucket/MiamiData.csv")
raw_nyc_data = pd.read_csv("airbnb-listings-bucket/NYCData.csv")
raw_sanfransisco_data = pd.read_csv("airbnb-listings-bucket/SanFransiscoData.csv")
raw_sydney_data = pd.read_csv("airbnb-listings-bucket/SydneyData.csv")
raw_tokyo_data = pd.read_csv("airbnb-listings-bucket/TokyoData.csv")
raw_toronto_data = pd.read_csv("airbnb-listings-bucket/TorontoData.csv")


# upload to S3_Bucket
s3_client = boto3.client('s3',
                         aws_access_key_id = aws_access_key,
                         aws_secret_access_key =aws_secret_key
                        )


response = s3_client.create_bucket(
            Bucket = 'airbnb-listings-bucket',
            CreateBucketConfiguration = {
                'LocationConstraint': 'eu-west-2',
            },
            )
print(response)

# upload to S3 bucket
def upload_to_s3(df, Bucket, object_key):
    try:
        buffer = io.BytesIO()
        df.to_csv(buffer, index = False)
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, Bucket, object_key)
        print(f"file uploaded to s3://{Bucket}/{object_key}.csv")
    except NoCredentialsError:
        print("Credentials not available or incorrect")
    except Exception as e:
        print(f"Upload to s3://{Bucket}/{object_key} failed: {e}")
        


# store all dfs in a dictionary

raw_data = {
    "dubaiData": raw_dubai_data,
    "losAngelesData": raw_la_data,
    "londonData": raw_london_data ,
    "miamiData": raw_miami_data,
    "newYorkCityData": raw_nyc_data,
    "sanFranciscoData": raw_sanfransisco_data,
    "sydneyData": raw_sydney_data,
    "tokyoData": raw_tokyo_data,
    "torontoData": raw_toronto_data
}


for df_name, df in raw_data.items():
    bucket = "airbnb-listings-bucket-bucket/raw-data/"
    object_key = df_name
    upload_to_s3(df, bucket, df_name )
    


