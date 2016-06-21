#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${DIR}

spark-submit \
  --master local[*] \
  --packages org.mongodb.spark:mongo-spark-connector_2.10:0.4 \
  ${DIR}/src/main/python/movie-recommendations.py
