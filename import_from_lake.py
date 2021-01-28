from boto3.session import Session
import boto3
import pandas as pd
import os

# Defining a frame to get credentials from AWS file

cred = pd.read_csv("data/crds/new_user_credentials.csv")

ACCESS_KEY, SECRET_KEY = cred[['Access key ID','Secret access key']].iloc[0].values
BUCKET_NAME = 'csgodatas3'

session = Session(aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY)

s3 = session.resource('s3')

bucket = s3.Bucket(BUCKET_NAME)
parquets = []

# Importing Bronze Layer

for file in bucket.objects.all():
    if(file.key[-7:] == 'parquet'):
        parquets.append(file.key)

currentBronzeParquets = os.listdir('data/matches/bronze/')
currentSilverParquets = os.listdir('data/matches/silver/')


for parquet in parquets:

    # Importing Bronze

    if(parquet.split('/')[1] == 'bronze'):
        parquetName = parquet.split('/bronze/')[1]
        if(parquetName not in currentBronzeParquets):
            bucket.download_file(parquet, 'data/matches/bronze/'+parquetName)

    # Importing Silver 

    elif(parquet.split('/')[1] == 'silver'):

        parquetName = 'part'+parquet.split('part')[1]
        dirName = 'data/matches/'+parquet.split('part')[0].split("pipelineExport/")[1]
 
        if(not os.path.exists(os.path.dirname(dirName))):
            os.makedirs(os.path.dirname(dirName))
            
        if(parquetName not in currentSilverParquets):
            bucket.download_file(parquet, dirName+parquetName)
