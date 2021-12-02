''' Query #4 '''
# Import libraries
import pyspark
from pyspark.sql.functions import substring, length, col, expr, to_timestamp
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pprint import pprint

# Create spark objects:
spark = SparkSession.builder.appName('Final_Project').getOrCreate()
sc = spark.sparkContext
sql = SQLContext(sc)

# Create a dataframe:
df = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("gs://cs488-project--flat-atlas-bucket/freeway_data/processed_data.csv"))
df.cache()

# Convert starttime to timestamp:
df3 = df.withColumn("starttime", \
                    expr("substring(starttime, 1, length(starttime)-3)") \
                    .cast('timestamp'))

# Create a temp view to use in queries:
df3.createOrReplaceTempView("df_all")

#Query the length of the Foster NB station:
query_len = ''' SELECT DISTINCT length FROM df_all
                WHERE locationtext = "Foster" AND
                shortdirection = "N" '''
len_df = spark.sql(query_len)
station_len = len_df.collect()[0][0]

# Create dictionary to hold result data:
travel_times = {}

# Filter the Foster Rd NB data for 9/22/11 from 7 AM - 9 AM:
query_morn = '''SELECT starttime, speed FROM df_all WHERE locationtext = 'Foster'
                AND starttime >= '2011-09-22 07:00:20'
                AND starttime <= '2011-09-22 09:00:00'
                AND shortdirection = 'N'
                AND volume > 0 AND speed > 0 '''
df_morn = spark.sql(query_morn)

# Find the average speed for the morning peak:
avg_morn = df_morn.agg({'speed': 'avg'})

# Convert morning travel time to seconds:
travel_times['7 AM - 9 AM'] = \
        (float(station_len) / avg_morn.collect()[0][0]) * 3600

# Filter the Foster Rd NB data for 9/22/11 from 4 PM - 6 PM:
query_eve = '''SELECT starttime, speed FROM df_all WHERE locationtext = 'Foster'
                AND starttime >= '2011-09-22 16:00:20'
                AND starttime <= '2011-09-22 18:00:00'
                AND shortdirection = 'N'
                AND volume > 0 AND speed > 0 '''
df_eve = spark.sql(query_eve)

# Find the average speed for the evening peak:
avg_eve = df_eve.agg({'speed': 'avg'})

# Convert evening travel time to seconds:
travel_times['4 PM - 6 PM'] = \
        (float(station_len) / avg_eve.collect()[0][0]) * 3600

print("Travel times for I-205 Foster NB station for peak hours:")
for time in travel_times:
    print(time, ' : ', travel_times[time])

