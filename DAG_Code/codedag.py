from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
import sys
sys.path.insert(0, '/root/airflow/code')

from logistic_regression import LRClassifier
from random_forest_classifier import RFClassifier
from SVM import SVMClassifier



def compare(ti):
    results=ti.xcom_pull(task_ids=["logistic_regression","svm_classifier","rf_classifier"])
    if not results:
        raise ValueError('No Values found in Xcoms')

    with open('/root/airflow/results/comparison_results.txt','w') as f:
        f.write("logistic regression performance:\n {0}".format(results[0]))
        f.write("SVM classifier performance:\n {0}".format(results[1]))
        f.write("RF classifier performance:\n {0}".format(results[2]))

with DAG(dag_id='sentiment_analysis_workflow', schedule_interval='@daily',start_date=datetime(year=2023, month=4, day=20),catchup=False ) as dag:

    S3_to_local_download=BashOperator(
        task_id='S3_to_local_download',
        bash_command='python3  /root/airflow/code/download.py '
    )

    local_to_hdfs_move=BashOperator(
        task_id='local_to_hdfs_move',
        bash_command='bash /root/airflow/code/local_to_hdfs.sh '
    )

    pre_processing=BashOperator(
        task_id='pre_processing',
        bash_command='python3 /root/airflow/code/preprocessing.py '
    )

    logistic_regression=PythonOperator(
        task_id='logistic_regression',
        python_callable=LRClassifier,
        #python_callable=x1,
        do_xcom_push=True
    )

    SVM_classifier=PythonOperator(
        task_id='svm_classifier',
        python_callable=SVMClassifier,
        #python_callable=x1,
        do_xcom_push=True
    )
    
    RF_classifier=PythonOperator(
        task_id='rf_classifier',
        python_callable=RFClassifier,
        #python_callable=x1,
        do_xcom_push=True
    )
    
    comparison_results=BranchPythonOperator(
        task_id='comparison_results',
        python_callable=compare
    )


    S3_to_local_download >> local_to_hdfs_move >> pre_processing >> [logistic_regression,SVM_classifier,RF_classifier] >> comparison_results