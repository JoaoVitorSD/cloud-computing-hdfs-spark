from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, exp, desc, row_number
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("HelloLines").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load data
tracks = spark.read.json("hdfs://localhost:9000/datasets/spotify/tracks.json")
# Table before removing outliers
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
# Calculating IQR
initial_size =tracks.count()
quantiles = tracks.approxQuantile("duration_ms", [0.25, 0.75],0.0);
q1, q3 = quantiles[0], quantiles[1]
iqr = q3 - q1
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr
# Removing outliers
tracks = tracks.filter((col("duration_ms") >= lower_bound) & (col("duration_ms") <= upper_bound))
outliers = initial_size - tracks.count()
# Table after removing outliers
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

spark.stop()