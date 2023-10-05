from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, exp, desc, row_number,count
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("HelloLines").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

playlists = spark.read.json("hdfs://localhost:9000/datasets/spotify/playlist.json")
tracks =   spark.read.json("hdfs://localhost:9000/datasets/spotify/tracks.json")


size = playlists.join(tracks, playlists["pid"] == tracks["pid"], ).groupBy(playlists["pid"]).count().alias("count_songs")
artist_counts = playlists.join(tracks, playlists["pid"] == tracks["pid"], ) \
                .groupBy(playlists["pid"],"artist_uri").agg(count("artist_uri")
.alias("artist_count")).orderBy(desc("artist_count")).limit(5).show()

max_artist_count = artist_counts.groupBy("pid").agg(max(col("artist_count")).alias("max_artist_count"))

prevalence_df = size.join(max_artist_count, "pid").withColumn(
    "artist_prevalence", col("max_artist_count") / col("artist_ocurrences")
).select("pid", "artist_prevalence")

# Show the resulting DataFrame
prevalence_df.show()


spark.close()