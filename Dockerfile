FROM jupyter/pyspark-notebook

MAINTAINER Anil Choudhary <anil.iiitm@gmail.com>

# Set PYSPARK_HOME in the python2 spec
RUN jq '.["env"]["PYSPARK_SUBMIT_ARGS"]="--master spark://127.0.0.1:8888 --jars  /mnt/jars/guava-19.0.jar,/mnt/jars/gcs-connector-1.4.5-hadoop2-shaded.jar,/mnt/jars/gson-2.6.2.jar,/mnt/jars/bigquery-connector-0.7.5-hadoop2.jar,/mnt/jars/gcsio-1.4.5.jar,/mnt/jars/util-1.4.5.jar,/mnt/jars/util-hadoop-1.4.5-hadoop2.jar,/mnt/jars/google-http-client-1.21.0.jar,/mnt/jars/google-http-client-jackson2-1.21.0.jar,/mnt/jars/google-api-services-bigquery-v2-rev282-1.21.0.jar pyspark-shell"' $CONDA_DIR/share/jupyter/kernels/python2/kernel.json > /tmp/kernel.json && mv /tmp/kernel.json $CONDA_DIR/share/jupyter/kernels/python2/kernel.json
