print(99)

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("HelloLines") \
    .getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile("hdfs://localhost:9000/datasets/spotify/tracks.json")
df = spark.read.json(rdd)
print(df["duration_ms"][1])
sc.stop()

print(99)