from nltk.corpus import stopwords
import string
import re
import nltk
import sklearn
from nltk.corpus import stopwords
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,ArrayType
from pyspark.sql.functions import col, udf,trim,regexp_replace ,rand
from pyspark.ml.feature import StopWordsRemover,Tokenizer,HashingTF, IDF ,StringIndexer
from nltk.stem.snowball import SnowballStemmer
from sklearn.metrics import classification_report
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import findspark
findspark.init("/usr/local/spark-2.4.4-bin-hadoop2.7")

spark=SparkSession.builder.appName('Newone').getOrCreate()

schema=StructType([StructField("SId",IntegerType(),False),\
                   StructField("target",IntegerType(),False),\
                   StructField("text",StringType(),False)])


df = spark.read.format('org.apache.spark.sql.json') \
        .option("header",True)\
        .option("schema",schema)\
        .option("master",'local[*]')\
        .option("encoding",'ISO-8859-1')\
        .option("linesep",'\n')\
        .option("numPartitions", 16)\
        .load(r"/user/root/input_data/sentiment_analysis_dataset.json")



new_df=df.select(['text','target']).dropna()

new_df=new_df.withColumn('rand',rand(seed=50)).orderBy('rand')
new_df=new_df.drop('rand')

#new_df=new_df.limit(1000000)

def pre_processing(df, column_name):
    
    df=df.withColumn(column_name, trim(col(column_name)))     # removing spaces from beginning and end of the string
    df=df.withColumn(column_name, regexp_replace(col(column_name), r'((www.\S+)|(https?://\S+)|(htpp?://\S+))', r""))  # removing additonal unneccessary information
    df=df.withColumn(column_name,regexp_replace(col(column_name),r'[0-9]\S+', r""))   # removing numbers
    df=df.withColumn(column_name,regexp_replace(col(column_name),r'(@\S+) | (#\S+)',r"")) # remmoving unneccessary characters  like @ ,# 
    df=df.withColumn(column_name, regexp_replace(col(column_name), "[^a-zA-Z\\s]", ""))   # removing puncuations 
    df=df.withColumn(column_name, trim(regexp_replace(col(column_name), " +", " ")))      
    
    return df
    

def tokenize(df, column_name):
  tokenizer = Tokenizer(inputCol=column_name, outputCol="tokens")
  return tokenizer.transform(df).cache()

new_df=pre_processing(new_df,'text')
new_df=tokenize(new_df,'text').drop('text')


nltk.download('stopwords')
stop_words = list(stopwords.words("english"))

def stopword_remover(df, tokens_column_name):
  stopwords_remover = StopWordsRemover(inputCol=tokens_column_name, outputCol="new_tokens",stopWords=stop_words)
  return stopwords_remover.transform(df).cache()



new_df=stopword_remover(new_df,'tokens').drop('tokens')
#new_df.show(10,truncate=False)


def stem(df, terms_column_name):
    stemmer = SnowballStemmer(language="english")
    stemmer_udf = udf(lambda tokens: [stemmer.stem(token) for token in tokens], ArrayType(StringType()))
    terms_stemmed_df = df.withColumn("stemmed_tokens", stemmer_udf(terms_column_name)).cache()
    return terms_stemmed_df


new_df=stem(new_df,'new_tokens').drop('new_tokens').dropna()

hashtf = HashingTF(numFeatures=2**16, inputCol="stemmed_tokens", outputCol='tf')
idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
label_stringIdx = StringIndexer(inputCol = "target", outputCol = "label")
pipeline = Pipeline(stages=[ hashtf, idf, label_stringIdx])


pipelineFit = pipeline.fit(new_df)
new_df = pipelineFit.transform(new_df)

new_df=new_df.drop('target','stemmed_tokens','tf')

train_df,test_df=new_df.randomSplit([0.8,0.2],seed=100)


train_df.write.mode("overwrite").partitionBy("label").parquet("/user/root/processed_dataset/train.parquet")
test_df.write.mode("overwrite").partitionBy("label").parquet("/user/root/processed_dataset/test.parquet")
