from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, exp, desc, row_number,count, max,format_number
import matplotlib.pyplot as plt
import numpy as np


spark = SparkSession.builder.appName("HelloLines").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load data

playlists = spark.read.json("hdfs://localhost:9000/datasets/spotify/playlist.json")
tracks =   spark.read.json("hdfs://localhost:9000/datasets/spotify/tracks.json")
playlist_count = playlists.count()


# Calculate the number of songs in each playlist
artist_counts = playlists.join(tracks, playlists["pid"] == tracks["pid"], ) \
                .groupBy(playlists["pid"],"artist_uri", "artist_name").agg(count("artist_uri") \
                .alias("artist_count"))

# Select the maximum number of songs by artist in each playlist
max_artist_count = artist_counts.groupBy("pid", "artist_uri", "artist_name").agg(max(artist_counts.artist_count).alias("max_artist_count"))

# Calcula a razão entre o número de músicas de um artista e o número total de músicas da playlist
prevalence_df = playlists.join(max_artist_count, "pid").withColumn(
    "artist_prevalence", format_number((col("max_artist_count") / playlists.num_tracks)*100,2)
).select(playlists["pid"], "artist_prevalence").orderBy("artist_prevalence")

window_spec = Window.orderBy("artist_prevalence")
prevalence_df = prevalence_df.withColumn("row_number", row_number().over(window_spec))

# Calculate the total number of rows in the DataFrame
total_rows = prevalence_df.count()

# Calculate the CDF values
sorted_df = prevalence_df.withColumn("cdf", (col("row_number") - 1) / total_rows)

# Collect the CDF values as a Pandas DataFrame for plotting
cdf_df = sorted_df.select("artist_prevalence", "cdf").toPandas()

# Plot the CDF
custom_intervals = [0, 0.25, 0.5, 0.75, 1.0]

plt.figure(figsize=(8, 6))
plt.plot(cdf_df["artist_prevalence"], cdf_df["cdf"])
plt.yticks(custom_intervals)
plt.xticks(custom_intervals)
plt.title("CDF")
plt.xlabel("Artist Prevalence")
plt.ylabel("CDF")
plt.show()

# Stop the SparkSession when done
spark.stop()
