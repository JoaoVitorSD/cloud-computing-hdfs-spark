from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, exp, desc, row_number, when, year,from_unixtime, count
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("HelloLines").config("spark.executor.instances", "2").config("spark.executor.cores", "2").config("spark.executor.memory", "1024M").getOrCreate()
sc = spark.sparkContext

# Load data

playlists = spark.read.json("hdfs://localhost:9000/datasets/spotify/playlist.json")
tracks =   spark.read.json("hdfs://localhost:9000/datasets/spotify/tracks.json")


# Get the top 5 most popular artists
popular_artists = tracks.join(playlists, tracks["pid"] == playlists["pid"], ).groupBy("artist_uri", "artist_name").agg(count("artist_uri").alias("ocurrences")).orderBy(desc("ocurrences")).limit(5)

# Renaming the column
popular_artists = popular_artists.select(col("artist_uri").alias("top_artist_uri"))

# Agrouping ocurrences by year
popular_artists = tracks.join(popular_artists, popular_artists["top_artist_uri"] == tracks["artist_uri"]).join(playlists, tracks["pid"] == playlists["pid"] ).withColumn("year", year(from_unixtime(col("modified_at")))).groupBy("year", tracks["artist_name"]).agg(count(tracks["artist_uri"]).alias("ocurrences")).orderBy("year")

# Plotting the graph
pandas_df = popular_artists.toPandas()

for artist, data in pandas_df.groupby('artist_name'):
    plt.plot(data['year'], data['ocurrences'], label=artist)

plt.title("Finding the most popular artists over time")
plt.ylabel('Occurrences Along Year')
plt.xlabel('Year')
plt.legend()
plt.show()

spark.stop()