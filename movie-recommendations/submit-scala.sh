#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${DIR}

./sbt clean package
spark-submit \
  --class example.MovieRecommendation \
  --master local[*] \
  --packages org.mongodb.spark:mongo-spark-connector_2.10:0.4 \
  ${DIR}/target/scala-2.10/movieRecommendations_2.10-0.1.jar
