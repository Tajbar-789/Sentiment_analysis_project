# Sentiment_analysis_project
This repository contains the code for sentiment analysis data pipeline which is hosted on a EC2 Machine on pyspark-hadoop environment .

## Architechture

![architechture](https://github.com/Tajbar-789/Sentiment_analysis_project/assets/54442879/1346b2b4-1c61-4475-af1d-cd69a8afb953)

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

## Data Pipeline
