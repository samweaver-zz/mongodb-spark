# Movie recommendations

An example of using the MongoDB Spark Connector and the [Movielens](http://grouplens.org/datasets/movielens/) dataset to recommend films.

### Pre Reqs
* Download and install Spark (this example uses Spark 1.6.x) and have spark-submit on the path
* Download and install MongoDB (this example uses v3.2.x)
* Download the data/movies.zip and mongorestore it into mongodb

### What the code does?
* Reads from the `movies.movie_ratings` collection
* Reads from the `movies.personal_ratings` collection
* Creates a list of recommendations for the user based on their personal ratings
* Saves those ratings back out to MongoDB in the `movies.user_recommendations` collection

### Running the examples

There are two examples:
  - [MovieRecommendation.scala](src/main/scala/example/MovieRecommendation.scala)
  - [movie-recommendations.py](src/main/python/movie-recommendations.py)
  
To run one of them, refer to the appropriate script:
  - [submit-scala.sh](submit-scala.sh) for Scala
  - [submit-python.sh](submit-python.sh) for Python


Questions? sam.weaver[at]mongodb[dot]com
