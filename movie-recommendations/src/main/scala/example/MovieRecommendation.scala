package example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}

/**
 * Represents a Users movie rating
 */
case class UserMovieRating(user_id: Int, movie_id: Int, rating: Double)

object MovieRecommendation {

  /**
   * Run this main method to see the output of this quick example or copy the code into the spark shell
   *
   * @param args takes an optional single argument for the connection string
   * @throws Throwable if an operation fails
   */
  def main(args: Array[String]): Unit = {
    // Turn off noisy logging
    Logger.getLogger("org").setLevel(Level.WARN)

    // Set up configurations
    val sc = getSparkContext()
    val sqlContext = SQLContext.getOrCreate(sc)

    val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/movies.movie_ratings?readPreference=primaryPreferred"))
    val writeConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/movies.user_recommendations"))
    val userId = 0

    // Load the movie rating data
    val movieRatings = MongoSpark.load(sc, readConfig).toDF[UserMovieRating]

    // Create the ALS instance and map the movie data
    val als = new ALS()
      .setCheckpointInterval(2)
      .setUserCol("user_id")
      .setItemCol("movie_id")
      .setRatingCol("rating")

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using the ALS evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(als.regParam, Array(0.1, 10.0))
      .addGrid(als.rank, Array(8, 10))
      .addGrid(als.maxIter, Array(10, 20))
      .build()

    // Set the training and validation split - 80% of the data will be used for training and the remaining 20% for validation.
    val trainedAndValidatedModel = new TrainValidationSplit()
      .setEstimator(als)
      .setEvaluator(new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction"))
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.8)

    // Calculating the best model
    val bestModel = trainedAndValidatedModel.fit(movieRatings)

    // Combine the datasets
    val userRatings = MongoSpark.load(sc, readConfig.copy(collectionName = "personal_ratings")).toDF[UserMovieRating]
    val combinedRatings = movieRatings.unionAll(userRatings)

    // Retrain using the combinedRatings
    val combinedModel = als.fit(combinedRatings, bestModel.extractParamMap())

    // Get user recommendations
    import sqlContext.implicits._
    val unratedMovies = movieRatings.filter(s"user_id != $userId").select("movie_id").distinct().map(r =>
      (userId, r.getAs[Int]("movie_id"))).toDF("user_id", "movie_id")
    val recommendations = combinedModel.transform(unratedMovies)

    // Convert the recommendations into UserMovieRatings
    val userRecommendations = recommendations.map(r =>
      UserMovieRating(0, r.getAs[Int]("movie_id"), r.getAs[Float]("prediction").toInt)).toDF()

    // Save to MongoDB
    MongoSpark.save(userRecommendations.write.mode("overwrite"), writeConfig)

    // Clean up
    sc.stop()
  }

  /**
   * Gets or creates the Spark Context
   */
  def getSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MovieRatings")

    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir("/tmp/checkpoint/")
    sc
  }
}
