from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HelloLines").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile("hdfs://localhost:9000/datasets/spotify/")
lines = rdd.count()
outrdd = sc.parallelize([lines])
# The following will fail if the output directory exists:
sc.stop()