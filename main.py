from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, exp, desc, row_number
import matplotlib.pyplot as plt



spark = SparkSession.builder.appName("HelloLines").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# STEP - 1 - Songs Duration 
tracks = spark.read.json("hdfs://localhost:9000/datasets/spotify/tracks.json")
# Tabela antes da filtragem de outlier
minimun = tracks.agg({"duration_ms": "min"}).collect()[0][0]
maximum = tracks.agg({"duration_ms": "max"}).collect()[0][0]
average = tracks.agg({"duration_ms": "avg"}).collect()[0][0]
data = {
    "Metrics": ["Minimum Duration", "Average Duration", "Maximum Duration"],
    "Values": [minimun, average, maximum]
}
fig, ax = plt.subplots()
ax.axis('tight')
ax.axis('off')
ax.table(cellText=[data["Values"]], colLabels=data["Metrics"], cellLoc='center', loc='center')
plt.show()
# Removing outliers
initial_size =tracks.count()
quantiles = tracks.approxQuantile("duration_ms", [0.25, 0.75],0.0);
q1, q3 = quantiles[0], quantiles[1]
iqr = q3 - q1
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr
tracks = tracks.filter((col("duration_ms") >= lower_bound) & (col("duration_ms") <= upper_bound))
outliers = initial_size - tracks.count()
# Create a table using matplotlib
minimun = tracks.agg({"duration_ms": "min"}).collect()[0][0]
maximum = tracks.agg({"duration_ms": "max"}).collect()[0][0]
average = tracks.agg({"duration_ms": "avg"}).collect()[0][0]
data = {
    "Metrics": ["Minimum Duration", "Average Duration", "Maximum Duration", "Outliers"],
    "Values": [minimun, average, maximum, outliers]
}
fig, ax = plt.subplots()
ax.axis('tight')
ax.axis('off')
ax.table(cellText=[data["Values"]], colLabels=data["Metrics"], cellLoc='center', loc='center')
plt.show()

# Generating statistics duration
playlists = spark.read.json("hdfs://localhost:9000/datasets/spotify/playlist.json")
# window = Window.partitionBy("pid").orderBy(desc("last_modification"))
# playlists = playlists.withColumn("row_number", row_number().over(window))
# playlists = playlists.filter(playlists["row_number"] == 1).drop("row_number")
# TODO pegar a última ocorrência de cada plalist pela data

tracks =   spark.read.json("hdfs://localhost:9000/datasets/spotify/tracks.json")
tracks = tracks.join(playlists, tracks["pid"] == playlists["pid"], ).groupBy("artist_uri", "artist_name").count()
tracks = tracks.orderBy(desc("count"))
tracks.limit(5).show()


spark.stop()


# 