#!/usr/bin/env python

import sys
import itertools
import time
from math import sqrt
from operator import add
from os.path import join, isfile, dirname

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS
from pyspark.sql import SQLContext

def init_spark_context():
    conf = SparkConf().setAppName("MovieRatings").set("spark.executor.memory", "4g")
    sc = SparkContext(conf=conf)
    sc.setCheckpointDir('/tmp/checkpoint/')
    return sc

def find_best_model(data):
    global bestRank
    global bestLambda
    global bestNumIter
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1    
    ranks = [8, 12]
    lambdas = [0.1, 10.0]
    numIters = [10, 20]    
    min_error = float('inf')
    training, validation, test = data.randomSplit([0.6, 0.2, 0.2], 6) 
    for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
        ALS.checkpointInterval = 2
        training_data = training.map(lambda xs: [int(x) for x in xs]) 
        model = ALS.train(training_data, rank, numIter, lmbda)
        validation_data = validation.map(lambda p: (int(p[0]), int(p[1])))
        predictions = model.predictAll(validation_data).map(lambda r: ((r[0], r[1]), r[2]))
        ratings_and_predictions = validation.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
        error = sqrt(ratings_and_predictions.map(lambda r: (r[1][0] - r[1][1])**2).mean())
        print 'For rank %s the RMSE is %s' % (rank, error)
        if error < min_error:
            min_error = error
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter
    print 'The best model was trained with rank %s' % bestRank

def get_user_recommendations(personal_ratings, complete_data, model):
    a = personal_ratings.map(lambda x: (x[1]))
    b = complete_data_with_user_ratings.map(lambda x: (x[1]))
    user_unrated_movies = b.subtract(a)
    user_unrated_movies = user_unrated_movies.map(lambda x: (0, x)).distinct()
    user_recommendations = new_ratings_model.predictAll(user_unrated_movies).map(lambda r: (r[0], r[1], r[2]))
    return user_recommendations

def combine_data(personal_ratings, data):
    personal_ratings = personal_ratings.map(lambda xs: [int(x) for x in xs])
    data = data.map(lambda xs: [int(x) for x in xs])
    d = data.union(personal_ratings) 
    return d

if __name__ == "__main__":  
   
    sc = init_spark_context()
    sqlContext = SQLContext(sc)
   
    # Read movies collection and select only fields we care about
    df = sqlContext.read.format("com.mongodb.spark.sql").load()
    data = df.select('user_id', 'movie_id','rating').repartition(16).cache()
    
    find_best_model(data)

    # Next we get the personal ratings from the personal ratings_collection
    df = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "personal_ratings").load()
    personal_ratings = df.select('user_id', 'movie_id','rating').repartition(16).cache()

    # Combine personal ratings with existing data
    complete_data_with_user_ratings = combine_data(personal_ratings, data)

    # Train new model
    new_ratings_model = ALS.train(complete_data_with_user_ratings, bestRank, bestNumIter, bestLambda)
    user_recommendations = get_user_recommendations(personal_ratings, complete_data_with_user_ratings, new_ratings_model)
    
    # Make a DataFrame. Save to MongoDB
    r = sqlContext.createDataFrame(user_recommendations, ['user_id', 'movie_id', 'rating'])
    r.write.format("com.mongodb.spark.sql.DefaultSource").option("collection","user_recommendations").mode("overwrite").save()
    
    #Clean up
    sc.stop()