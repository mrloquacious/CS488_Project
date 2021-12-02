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

# Convert starttime to timestamp:
df3 = df.withColumn("starttime", expr("substring(starttime, 1, length(starttime)-3)") .cast('timestamp'))

# Create a temp view to use in queries:
df3.createOrReplaceTempView("df4")

# Filter the Foster Rd NB data for 9/15/11
df5 = spark.sql('''SELECT * FROM df4 WHERE locationtext = 'Foster'
                AND starttime > '2011-09-15 00:00:05' 
                AND starttime <= '2011-09-16 00:00:00'
                AND shortdirection = 'N' 
                AND volume > 0 AND speed > 0''')

# Create temp view of records for 9/15/11:
df5.createOrReplaceTempView("df_day")

#Query the length of the Foster NB station:
query_len = ''' SELECT DISTINCT length FROM df_day
        WHERE locationtext = "Foster" AND
        shortdirection = "N" '''
len_df = spark.sql(query_len)
station_len = len_df.collect()[0][0]

# Create dictionary to hold result data:
travel_times = {}

# Loop over data in 5 minute increments:
for i in range(0, 120, 5):
    # Query to get average speed and end time for 5 minute period:
    query_5mins = f'''SELECT starttime, speed from df_day
                WHERE starttime > "2011-09-15 00:00:00" + INTERVAL {i} minutes
                AND starttime <= "2011-09-15 00:00:00" + INTERVAL {i + 5} minutes '''
    # Get the dataframe for the 5 minute period:
    rows = spark.sql(query_5mins)
    # Create temp view for the period:
    rows.createOrReplaceTempView("df_5min")
    # Extract the last timestamp for the 5 minute period and the average speed:
    endtime = rows.agg({'starttime': 'max'})
    avg = rows.agg({'speed': 'avg'})

    travel_times[str(endtime.collect()[0][0])] = \
        (float(station_len) / avg.collect()[0][0]) * 3600

pprint(travel_times)
