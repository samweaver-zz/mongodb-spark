

Submit job:
./bin/spark-submit --master "local[4]" --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/movies.movie-ratings?readPreference=primaryPreferred" --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/movies.movie-ratings" --packages org.mongodb.spark:mongo-spark-connector_2.10:0.4 --repositories https://oss.sonatype.org/content/repositories/snapshots
