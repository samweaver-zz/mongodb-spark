

### Submit job
```sh
$ ./bin/spark-submit --master "local[4]" --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/movies.movie-ratings?readPreference=primaryPreferred" --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/movies.movie-ratings" --packages org.mongodb.spark:mongo-spark-connector_2.10:0.4 --repositories https://oss.sonatype.org/content/repositories/snapshots
```
### Pre Reqs
* Download and install Spark (this example uses Spark 1.6.x)
* Download and install MongoDB (this example uses v3.2.x)
* Download the movies.zip and mongorestore it into mongodb

### What the code does?
* Reads from the movies.movie-ratings collection
* Reads from the movies.personal-ratings collection
* Creates a list of recommendations for the user based on their personal ratings
* Saves those ratings back out to MongoDB in the movies.user_recommendations collection

Questions? sam.weaver[at]mongodb[dot]com
