#!/bin/bash

bash start-all.sh

hdfs dfs -mkdir -p /user/root/input_data/
hdfs dfs -put -f -d /root/airflow/data/sentiment_analysis_dataset.json /user/root/input_data/ 
