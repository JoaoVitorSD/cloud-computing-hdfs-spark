from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, exp, desc, row_number,count, max,format_number
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("HelloLines").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Carrega os dados
playlists = spark.read.json("hdfs://localhost:9000/datasets/spotify/playlist.json")
tracks =   spark.read.json("hdfs://localhost:9000/datasets/spotify/tracks.json")

# Calcula o número de músicas de cada playlist
size = playlists.join(tracks, playlists["pid"] == tracks["pid"], ).groupBy(playlists["pid"]).agg(count(tracks.pid).alias("count_songs"))

# Calcula o número de músicas de cada artista em cada playlist
artist_counts = playlists.join(tracks, playlists["pid"] == tracks["pid"], ) \
                .groupBy(playlists["pid"],"artist_uri", "artist_name").agg(count("artist_uri") \
                .alias("artist_count"))

# Seleciona o artista mais frequente de cada playlist
max_artist_count = artist_counts.groupBy("pid", "artist_uri", "artist_name").agg(max(artist_counts.artist_count).alias("max_artist_count"))

# Calcula a razão entre o número de músicas de um artista e o número total de músicas da playlist
prevalence_df = size.join(max_artist_count, "pid").withColumn(
    "artist_prevalence", format_number((col("max_artist_count") / size.count_songs)*100,2)
).select("pid", "artist_prevalence", "artist_name").orderBy(desc("artist_prevalence")).show()