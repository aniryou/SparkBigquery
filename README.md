# spark_bigquery
Big query connector for spark

Setup instructions:
- Install docker
- Run setup.sh

Manual:
- Run below commands in order:
  1. eval $(docker-machine env default)
  2. docker pull aniryou/spark_bigquery
  3. docker run -ti -p 8888:8888 -v <path_to_repo>:/mnt --name spark_bq aniryou/spark_bigquery
  4. docker-machine env default
     + use DOCKER_HOST ip address to access jupyter notebook on http://DOCKER_HOST:8888
  5. upload Iris and Wordcount notebooks and run same
