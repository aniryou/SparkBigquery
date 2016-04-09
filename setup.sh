#!/bin/bash

eval $(docker-machine env default)
docker pull aniryou/spark_bigquery:v1
docker run -d -p 8888:8888 -v .:/mnt aniryou/spark_bigquery:v1

