#!/usr/bin/python
"""BigQuery I/O PySpark example."""
import json
import pprint
import subprocess
import pyspark

sc = pyspark.SparkContext('local[*]')

# Use the Google Cloud Storage bucket for temporary BigQuery export data used
# by the InputFormat. This assumes the Google Cloud Storage connector for
# Hadoop is configured.
bq_project = "savvy-aileron-127413"
fs_project = "owners-681171445480"
bucket = "beatknow"
account_email = "savvy-aileron-127413@appspot.gserviceaccount.com"
input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input'.format(bucket)
localKeyfile = '/mnt/key.p12'

# Output Parameters
output_dataset = 'tutorial'
output_table = 'wordcount_table'

conf = {
    # Input Parameters
    'fs.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
    'fs.AbstractFileSystem.gs.impl': 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS',
    'fs.gs.project.id': fs_project,
    'mapred.bq.project.id': bq_project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': 'publicdata',
    'mapred.bq.input.dataset.id': 'samples',
    'mapred.bq.input.table.id': 'shakespeare',
    'google.cloud.auth.service.account.enable': 'true',
    'google.cloud.auth.service.account.email': account_email,
    'google.cloud.auth.service.account.keyfile': localKeyfile,
}

# Load data in from BigQuery.
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf)

# Perform word count.
word_counts = (
    table_data
    .map(lambda (_, record): json.loads(record))
    .map(lambda x: (x['word'].lower(), int(x['word_count'])))
    .reduceByKey(lambda x, y: x + y))

# Display 10 results.
pprint.pprint(word_counts.take(10))

# Stage data formatted as newline-delimited JSON in Google Cloud Storage.
output_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_output'.format(bucket)
partitions = range(word_counts.getNumPartitions())
output_files = [output_directory + '/part-{:05}'.format(i) for i in partitions]

(word_counts
 .map(lambda (w, c): json.dumps({'word': w, 'word_count': c}))
 .saveAsTextFile(output_directory))

# Manually clean up the staging_directories, otherwise BigQuery
# files will remain indefinitely.
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
output_path = sc._jvm.org.apache.hadoop.fs.Path(output_directory)
output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
    output_path, True)
