import boto3
import pandas as pd
import json 
import csv
import os
import requests
import sys
import seaborn as sns

url='https://www.kaggle.com/datasets/kazanova/sentiment140/download?datasetVersionNumber=2'
response = requests.get(url)
Folder_path=sys.argv[0]
File_name=sys.argv[1]
with open(os.path.join(Folder_path, File_name), 'wb') as f:
    f.write(response.content)

columns=['target','ids','date','flag','user','text']

sentiment_analysis_dataset=pd.read_csv("{0}/{1}".format(Folder_path,File_name) encoding = 'ISO-8859-1',quotechar="\"",header=None,names=columns)

sentiment_analysis_dataset.drop(columns=['ids','date','flag','user'],inplace=True)


sns.countplot(data=sentiment_analysis_dataset, x='target')

from wordcloud import WordCloud #Word visualization


## positive words cloud 
text_ = ""

word_cloud_text = ''.join(sentiment_analysis_dataset[sentiment_analysis_dataset["target"]==4].text)
#Creation of wordcloud
wordcloud = WordCloud(
    max_font_size=100,
    max_words=500,
    background_color="black",
    scale=10,
    width=1600,
    height=900
).generate(word_cloud_text)
#Figure properties
plt.figure(figsize=(20,16))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()

##negative words cloud

text_ = ""

word_cloud_text = ''.join(sentiment_analysis_dataset[sentiment_analysis_dataset["target"]==4].text)
#Creation of wordcloud
wordcloud = WordCloud(
    max_font_size=500,
    max_words=100,
    background_color="black",
    scale=10,
    width=1600,
    height=900
).generate(word_cloud_text)
#Figure properties
plt.figure(figsize=(20,16))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()

data={}

json_file=open("{0}/sentiment_analysis_dataset.json".format(Folder_path), 'w+',encoding='ISO-8859-1')

json_file.write('[')

for i in range(0,len(sentiment_analysis_dataset)):

    row={}

    key=str(i)

    row['text']=sentiment_analysis_dataset.iloc[i,1]

    row['target']=str(sentiment_analysis_dataset.iloc[i,0])
        
    
    json.dump(row,json_file)

    if(i<len(sentiment_analysis_dataset)-1):
        json_file.write(',\n')

json_file.write(']')    

json_file.close()

AWS_ACCESS_KEY_ID=sys.argv[2]
AWS_SECRET_ACCESS_KEY=sys.argv[3]
BUCKET_NAME=sys.argv[4]
KEY=sys.argv[5]

s3_client=boto3.client(service_name='s3',region_name='ap-south-1',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_client.upload_file('sentiment_analysis_dataset.json', BUCKET_NAME, KEY)

