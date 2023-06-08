from pyspark.sql import SparkSession
from sklearn.metrics import classification_report
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,ArrayType
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import findspark

def RFClassifier():
    findspark.init("/usr/local/spark-2.4.4-bin-hadoop2.7")
    spark=SparkSession.builder.appName('Newone').getOrCreate()


    schema=StructType([StructField("features",IntegerType(),False),\
                   StructField("label",StringType(),False)])


    train_df = spark.read.parquet(r"/user/root/processed_dataset/train.parquet/")
    test_df=spark.read.parquet(r"/user/root/processed_dataset/test.parquet/")




    rfc = RandomForestClassifier(featuresCol = 'features', labelCol = 'label',featureSubsetStrategy='log2',maxDepth=5,numTrees=50)
    rfcModel =rfc.fit(train_df)
    predictions = rfcModel.transform(test_df)

    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    print(evaluator.evaluate(predictions))

    labels=test_df.select('label').toPandas()["label"]


    predictions=predictions.select('prediction').toPandas()["prediction"]

    report=classification_report(labels,predictions)
    print("The classification Report for Logitic Regression Model:\n",report)

    
    return report

