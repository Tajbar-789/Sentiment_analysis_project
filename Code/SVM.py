from pyspark.sql import SparkSession
from sklearn.metrics import classification_report
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,ArrayType

from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import findspark 

def SVMClassifier():
    
    findspark.init("/usr/local/spark-2.4.4-bin-hadoop2.7")
    spark=SparkSession.builder.appName('Newone').getOrCreate()


    schema=StructType([StructField("features",IntegerType(),False),\
                   StructField("label",StringType(),False)])


    train_df = spark.read.parquet(r"/user/root/processed_dataset/train.parquet/")
    test_df=spark.read.parquet(r"/user/root/processed_dataset/test.parquet/")



    lsvc=LinearSVC( featuresCol='features',labelCol='label',maxIter=100,regParam=0.5, threshold=0.7)
    SVCModel = lsvc.fit(train_df)

    predictions = SVCModel.transform(test_df)

    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    print(evaluator.evaluate(predictions))



    labels=test_df.select('label').toPandas()["label"]

    predictions=predictions.select('prediction').toPandas()["prediction"]

    report=classification_report(labels,predictions)
    print("The classification Report for SVM Classifier Model:\n",report)

    return report

