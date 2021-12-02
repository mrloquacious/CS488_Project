# Import libraries
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

# Create spark context
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sql = SQLContext(sc)

# Create dataframes
df_highway = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("highways.csv"))

df_detector = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("freeway_detectors.csv"))

df_station = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("freeway_stations.csv"))

df_loopdata = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("freeway_loopdata.csv"))

# Drop unwanted columns
df_detector = df_detector.drop("milepost", "detectorclass","lanenumber")

df_station = df_station.drop("milepost","numberlanes","latlon","stationclass")

df_highway = df_highway.drop("direction")

df_loopdata = df_loopdata.drop("occupancy","status")

# Join the dataframes
df_first = df_highway.join(df_detector, on=['highwayid'], how='full')

# Drop highwayid to avoid duplicate
df_first = df_first.drop("highwayid")
df_first = df_first.drop("locationtext")

# Join to get the final result
df_second = df_first.join(df_station, on=['stationid'], how='full')
df_third = df_second.join(df_loopdata, on=['detectorid'], how='full')

from pyspark.sql.functions import regexp_replace
df_third = df_third.withColumn("locationtext", regexp_replace(df_third["locationtext"], " (N|E|S|W)B", "")) # remove directions
df_third = df_third.withColumn("locationtext", regexp_replace(df_third['locationtext'], " to ?", "")) # remove " to " and " to"
df_third = df_third.withColumn("locationtext", regexp_replace(df_third["locationtext"], " at ", ""))
df_third = df_third.withColumn("locationtext", regexp_replace(df_third["locationtext"], "I-205", ""))
df_third = df_third.withColumn("locationtext", regexp_replace(df_third["locationtext"], " Blvd", ""))
df_third = df_third.withColumn("locationtext", regexp_replace(df_third["locationtext"], " Cr", " Creek")) # expand CR to Creek

df_third = df_third.filter(df_third.dqflags.cast('int').bitwiseAND(16) == 0) # only keep row if bit position 5 in the dqflag is off (on means Speed = 0 and Volume > 0)
df_third = df_third.filter(df_third.dqflags.cast('int').bitwiseAND(32) == 0) # bit position 6 in the dqflag is off (on means Speed > 0 and Volume = 0)
df_third = df_third.filter(df_third.dqflags.cast('int').bitwiseAND(64) == 0) # bit position 7 in the dqflag is off (on means Occupancy > 0 and Volume = 0)
df_third = df_third.drop("dqflags") # not needed anymore


# Show the result
df_third.show()
# Convert to CSV
df_third.coalesce(1).write.option("header", "true").csv("processed_data")