import json
import pandas as pd
import boto3


s3_client=boto3.client(service_name='s3',region_name='ap-south-1',aws_access_key_id='AKIAUFGRY5J2WTGZFH45',aws_secret_access_key='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')
s3_client.download_file(Bucket='sentiment-analysis-project-bucket', Key='json_data.json',Filename='/root/airflow/data/sentiment_analysis_dataset.json')
