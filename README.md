# spark_bigquery
Big query connector for spark

Setup instructions:
- Install docker
- Run below commands in order:
  1. eval $(docker-machine env default)
  2. docker build -t aniryou/spark_bigquery:v1 .
  3. docker run -ti -p 8888:8888 -v $REPO_FULLPATH:/mnt --name spark_bq aniryou/spark_bigquery:v1
  4. docker-machine env default
     use DOCKER_HOST ip address to access jupyter notebook on http://DOCKER_HOST:8888
  5. upload Iris and Wordcount notebooks and run same
