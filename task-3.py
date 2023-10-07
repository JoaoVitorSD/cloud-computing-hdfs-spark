from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, exp, desc, row_number,count, max,format_number
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("HelloLines").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print(spark.sparkContext.uiWebUrl)

# Carrega os dados
playlists = spark.read.json("hdfs://localhost:9000/datasets/spotify/playlist.json")
tracks =   spark.read.json("hdfs://localhost:9000/datasets/spotify/tracks.json")
playlist_count = playlists.count()

# Calcula o número de músicas de cada playlist

# Calcula o número de músicas de cada artista em cada playlist
artist_counts = playlists.join(tracks, playlists["pid"] == tracks["pid"], ).groupBy(playlists["pid"],"artist_uri", "artist_name").agg(count("artist_uri").alias("artist_count"))

# Seleciona o artista mais frequente de cada playlist
max_artist_count = artist_counts.groupBy("pid").agg(max(artist_counts.artist_count).alias("max_artist_count"))

# Calcula a razão entre o número de músicas de um artista e o número total de músicas da playlist
prevalence_df = playlists.join(max_artist_count, "pid").withColumn("artist_prevalence", format_number((col("max_artist_count") / playlists.num_tracks)*100,2)).select(playlists["pid"], "artist_prevalence").orderBy(desc("artist_prevalence"))

# Calcula para cada playlist quantas playlists tem menos prevalencia que ela
prevalence_df_aux = prevalence_df

playlist_p = prevalence_df.join(prevalence_df_aux, prevalence_df["artist_prevalence"] > prevalence_df_aux["artist_prevalence"]).agg(count(prevalence_df_aux["pid"]).alias("total_playlist"))

# print("playlists: ", playlist_count)
# print("prevalences: ", prevalence_df.count())

# Calcula o x e o y do 
cdf = playlist_p.collect()
cdf_vector = []
prevalence = []
for playlist in cdf:
    # cdf = prevalence_df.filter(prevalence_df["artist_prevalence"] <= playlist.artist_prevalence).count()/playlist_count
    cdf_vector.append(cdf.total_playlist/playlist_count)
    prevalence.append(cdf.artist_prevalence)

# print("CDF: ")
# for i in cdf_vector:
#     print(i, end = ' ')

# print("Prevalence: ")
# for i in prevalence:
#     print(i, end = ' ')
    
plt.plot(prevalence, cdf_vector)
plt.savefig('teste.png', format='png')