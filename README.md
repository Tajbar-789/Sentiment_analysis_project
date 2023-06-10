# Sentiment_analysis_project
This repository contains the code for sentiment analysis data pipeline which is hosted on a EC2 Machine on pyspark-hadoop environment .

## Architechture

![architechture](https://github.com/Tajbar-789/Sentiment_analysis_project/assets/54442879/1346b2b4-1c61-4475-af1d-cd69a8afb953)

The twitter dataset is being pulled from kaggle into a local machine for EDA (Exploratory Data Analysis ) and some basic transformations using python . The transformed dataset is then uploaded to Amazon S3 storage for further processing . The dataset is then pulled into Hadoop pyspark environment that is hosted on AWS EC2 instance for preprocessing of the dataset and training of the ML models using the dataset . Apache Airflow is being used to run this complete workflow on this EC2 instance .

## Dataset
### Context
This is the sentiment140 dataset. It contains 1,600,000 tweets extracted using the twitter api . The tweets have been annotated (0 = negative, 4 = positive) and they can be used to detect sentiment .

### Content
It contains the following 6 fields:

1. target: the polarity of the tweet (0 = negative,  4 = positive)
2. ids: The id of the tweet ( 2087)
3. date: the date of the tweet (Sat May 16 23:58:44 UTC 2009)
4. flag: The query (lyx). If there is no query, then this value is NO_QUERY.
5. user: the user that tweeted (robotickilldozr)
6. text: the text of the tweet (Lyx i### s co### ol)

## Pre Processing

The Preprocessing of the dataset that is uploaded in Hadoop Pyspark environment is done in following  steps - 
1. Trimming and removing unnecessary characters in the tweets - The tweets are cleaned by removing extra spaces and unwanted characterss like puncuation marks ,numbers ,domain names etc .
2. Tokenization - The tweets are tranformed into a array of words or tokens .This step helps us to for further processing like stop words removal and stemming .
3. Stop Word removal - The tweets might contain some words that will not help the Machine Learning Models in decision making . These words for e.g. (The , I , They ,we etc) are to be removed in this step.
4. Stemming - The process of reducing inflection towards their root forms are called Stemming, this occurs in such a way that depicting a group of relatable words under the same stem, even if the root has no appropriate meaning. Stemming is a rule-based approach because it slices the inflected words from prefix or suffix as per the need using a set of commonly underused prefix and suffix, like “-ing”, “-ed”, “-es”, “-pre”, etc.
5. HashTF and IDF - HashTF Maps a sequence of terms to their term frequencies using the hashing trick. IDF computes the Inverse Document Frequency (IDF) given a collection of documents. These steps convert the stemmed tokens to features that can be used to train the ML models .

## Exploratory Data Analysis (EDA)
1. count plot showing the number of tweets of different types 

![__results___5_1](https://github.com/Tajbar-789/Sentiment_analysis_project/assets/54442879/e49e0e55-3283-4783-844b-d31db5e8fedc)

2. Word Cloud for postive tweets 

![__results___28_0](https://github.com/Tajbar-789/Sentiment_analysis_project/assets/54442879/03ac1e84-a4c0-4010-9a1a-7fe91d961d62)

3. word Cloud for negative tweets

![__results___30_0](https://github.com/Tajbar-789/Sentiment_analysis_project/assets/54442879/beac792c-98bb-4dfa-961e-b4df1f107bda)


## Airflow DAG

![Screenshot (27)](https://github.com/Tajbar-789/Sentiment_analysis_project/assets/54442879/a3344f08-ec4a-4dfa-8cc1-a4af7e679216)



