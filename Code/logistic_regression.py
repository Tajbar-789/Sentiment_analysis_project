from pyspark.sql import SparkSession
from sklearn.metrics import classification_report
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,ArrayType
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import findspark


def LRClassifier():
    findspark.init("/usr/local/spark-2.4.4-bin-hadoop2.7")

    spark=SparkSession.builder.appName('Newone').getOrCreate()


    schema=StructType([StructField("features",IntegerType(),False),\
                   StructField("label",StringType(),False)])


    train_df = spark.read.parquet(r"/user/root/processed_dataset/train.parquet/")
    test_df=spark.read.parquet(r"/user/root/processed_dataset/test.parquet/")




    lr = LogisticRegression(maxIter=1000)
    lrModel = lr.fit(train_df)
    predictions = lrModel.transform(test_df)

    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    print(evaluator.evaluate(predictions))




    labels=test_df.select('label').toPandas()["label"]


    predictions=predictions.select('prediction').toPandas()["prediction"]

    report=classification_report(labels,predictions)
    print("The classification Report for Logistic Regression Model:\n",report)

    return report
