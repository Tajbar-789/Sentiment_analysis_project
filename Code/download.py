import json
import pandas as pd
import boto3
import sys

AWS_ACCESS_KEY_ID=sys.argv[0]
AWS_SECRET_ACCESS_KEY=sys.argv[1]
BUCKET_NAME=sys.argv[2]
KEY=sys.argv[3]

s3_client=boto3.client(service_name='s3',region_name='ap-south-1',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_client.download_file(Bucket=BUCKET_NAME, Key=KEY,Filename='/root/airflow/data/sentiment_analysis_dataset.json')
