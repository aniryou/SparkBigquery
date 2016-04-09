#!/usr/bin/python
"""BigQuery I/O PySpark example."""
import json
import pprint
import subprocess
import pyspark

sc = pyspark.SparkContext('local[*]')

from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS

# Use the Google Cloud Storage bucket for temporary BigQuery export data used
# by the InputFormat. This assumes the Google Cloud Storage connector for
# Hadoop is configured.
bq_project = "savvy-aileron-127413"
fs_project = "owners-681171445480"
bucket = "spark_tmp"
account_email = "savvy-aileron-127413@appspot.gserviceaccount.com"
train_input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input_train'.format(bucket)
test_input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input_test'.format(bucket)
localKeyfile = '/mnt/key.p12'

MAP_FLOWER_NAME_TO_CODE = {
    'Iris-setosa': 0,
    'Iris-versicolor': 1,
    'Iris-virginica': 2
}

conf = {
    # Input Parameters
    'fs.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
    'fs.AbstractFileSystem.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS',
    'fs.gs.project.id': fs_project,
    'mapred.bq.project.id': bq_project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': train_input_directory,
    'mapred.bq.input.project.id': bq_project,
    'mapred.bq.input.dataset.id': 'iris',
    'mapred.bq.input.table.id': 'train',
    'google.cloud.auth.service.account.enable': 'true',
    'google.cloud.auth.service.account.email': account_email,
    'google.cloud.auth.service.account.keyfile': localKeyfile,
}

train_conf = conf

# Load train-data in from BigQuery.
train_table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=train_conf)

def featurize(x):
    return LabeledPoint(MAP_FLOWER_NAME_TO_CODE[x['class']],[x['sl'],x['sw'],x['pl'],x['pw']])

# extract LabeledPoint
train_data = (
    train_table_data
    .map(lambda (_, record): json.loads(record))
    .map(featurize)).collect()


model = LogisticRegressionWithLBFGS.train(sc.parallelize(train_data), iterations=1e2, numClasses=3)



test_conf = conf
test_conf['mapred.bq.input.table.id'] = 'test'
test_conf['mapred.bq.temp.gcs.path']  = test_input_directory

# Load test data in from BigQuery.
test_table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=test_conf)

# extract LabeledPoint
test_data = (
    test_table_data
    .map(lambda (_, record): json.loads(record))
    .map(featurize))


def predictionError(model, data):
    actualsAndPredictions = data.map(lambda p: (p.label, model.predict(p.features)))
    error = actualsAndPredictions.filter(lambda (actual, prediction): actual != prediction).count() / float(data.count())
    return error

test_error = predictionError(model, test_data)
print(test_error)
