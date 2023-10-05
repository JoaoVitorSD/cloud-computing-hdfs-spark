from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, exp, desc, row_number,count
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("HelloLines").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

playlists = spark.read.json("hdfs://localhost:9000/datasets/spotify/playlist.json")
tracks = spark.read.json("hdfs://localhost:9000/datasets/spotify/tracks.json")

tracks = tracks.join(playlists, tracks["pid"] == playlists["pid"], ).groupBy("artist_uri", "artist_name").count()
tracks = tracks.orderBy(desc("count"))
tracks.limit(5).show()


spark.stop()