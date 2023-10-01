from pyspark.sql import SparkSession
from pyspark.sql.functions import col, exp
spark = SparkSession.builder.appName("HelloLines").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext
# STEP - 1 - Songs Duration 
# Removing outliers
tracks = spark.read.json("hdfs://localhost:9000/datasets/spotify/tracks.json")
print("size "+ str(tracks.count()))
quantiles = tracks.approxQuantile("duration_ms", [0.25, 0.75],0.0);
q1, q3 = quantiles[0], quantiles[1]
iqr = q3 - q1
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr
tracks = tracks.filter((col("duration_ms") >= lower_bound) & (col("duration_ms") <= upper_bound))
print("size "+ str(tracks.count()))
sc.stop()
# Generating statistics duration

# STEP 