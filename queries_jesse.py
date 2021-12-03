import pyspark
from pyspark.sql.functions import substring, length, col, expr, to_timestamp
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pprint import pprint   

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sql = SQLContext(sc)

# Create dataframes
df_highway = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("gs://cs488-project--flat-atlas-bucket/freeway_data/processed_data.csv"))
df_highway.cache()

'''Query #1 '''
print("Query #1, count low speeds and high speeds:")
# SELECT COUNT(speed)
# WHERE speed < 5 OR speed > 80
print(df_highway.select('speed').where((df_highway.speed<5) | (df_highway.speed>80)).count())

'''Query #2 '''
print("\nQuery #2, total volume for the station Foster NB for Sept 15, 2011:")
# SELECT total volume
# WHERE locationtext == Foster
# AND shortdirection == N
# AND startime = Sept 15 (All Day)
volume = df_highway.filter((df_highway.locationtext == "Foster")
    & (df_highway.shortdirection=='N')
    & (df_highway.starttime > "2011-10-15 00:00:00-07")
    & (df_highway.starttime < "2011-10-16 00:00:00-07")).agg(F.sum(df_highway.volume))

print("Volume for Foster NB on Sept. 15, 2011:\n")
volume.show()

'''Query #3'''
# Convert starttime to timestamp:
df3 = df_highway.withColumn("starttime", expr("substring(starttime, 1, length(starttime)-3)") \
        .cast('timestamp'))

# Create a temp view to use in queries:
df3.createOrReplaceTempView("df_all")

# Filter the Foster Rd NB data for 9/15/11
df3b = spark.sql('''SELECT * FROM df_all WHERE locationtext = 'Foster'
                AND starttime > '2011-09-15 00:00:05' 
                AND starttime <= '2011-09-16 00:00:00'
                AND shortdirection = 'N' 
                AND volume > 0 AND speed > 0''')

# Create temp view of records for 9/15/11:
df3b.createOrReplaceTempView("df_day")

#Query the length of the Foster NB station:
query_len = ''' SELECT DISTINCT length FROM df_day
                WHERE locationtext = "Foster" AND
                shortdirection = "N" '''
len_df = spark.sql(query_len)
station_len = len_df.collect()[0][0]

# Create dictionary to hold result data:
travel_times = {}

# Loop over data in 5 minute increments:
for i in range(0, 1440, 5):
    # Query to get average speed and end time for 5 minute period:
    query_5mins = f'''SELECT starttime, speed from df_day
                    WHERE starttime > "2011-09-15 00:00:00" + INTERVAL {i} minutes
                    AND starttime <= "2011-09-15 00:00:00" + INTERVAL {i + 5} minutes '''
    # Get the dataframe for the 5 minute period:
    rows = spark.sql(query_5mins)
    # Extract the last timestamp for the 5 minute period and the average speed:
    endtime = rows.agg({'starttime': 'max'})
    avg = rows.agg({'speed': 'avg'})

    # Convert travel times to seconds:
    if avg.collect()[0][0]:
        travel_times[str(endtime.collect()[0][0])] = \
            (float(station_len) / avg.collect()[0][0]) * 3600
    else:
        travel_times[str(endtime.collect()[0][0])] = 'No Data' 

print("\nQuery #3 : Travel times for I-205 Foster NB station in 5 minute increments by end time")
for time in travel_times:
    print(time, ' : ', travel_times[time], " seconds\n")

'''Query #4'''
# Convert starttime to timestamp:
df4 = df_highway.withColumn("starttime", \
                    expr("substring(starttime, 1, length(starttime)-3)") \
                    .cast('timestamp'))

# Create a temp view to use in queries:
df4.createOrReplaceTempView("df_all2")

#Query the length of the Foster NB station:
query_len = ''' SELECT DISTINCT length FROM df_all2
                WHERE locationtext = "Foster" AND
                shortdirection = "N" '''
len_df = spark.sql(query_len)
station_len = len_df.collect()[0][0]

# Create dictionary to hold result data:
travel_times = {}

# Filter the Foster Rd NB data for 9/22/11 from 7 AM - 9 AM:
query_morn = '''SELECT starttime, speed FROM df_all2 WHERE locationtext = 'Foster'
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
query_eve = '''SELECT starttime, speed FROM df_all2 WHERE locationtext = 'Foster'
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

print("\nQuery #4 : Travel times for I-205 Foster NB station for peak hours")
for time in travel_times:
    print(time, ' : ', travel_times[time], ' seconds\n')

'''Query #5'''

print("\nQuery #5, average travel time for 7-9AM and 4-6PM on September 22, 2011 for the I-205 NB freeway:")
'''7am-9am'''
#Filter df so we work with only the data we need
filteredDf = df_highway.filter((df_highway.starttime > "2011-10-22 07:00:00-07") 
                              & (df_highway.starttime < "2011-10-22 09:00:00-07")
                              & (df_highway.highwayname=="I-205") 
                              & (df_highway.shortdirection=='N'))

filteredDf.cache()
#Determine avg detector speed
avgDetectorSpeeds = filteredDf.select('detectorid', 'speed').groupBy('detectorid').agg(F.avg('speed')).collect()
#Convert from list to df
R = Row('detectorid', 'speed')
#Create df
df_avgDetectorSpeed = spark.createDataFrame([R(x,y) for x, y in avgDetectorSpeeds])

#Determine which detectors are associated with which stations
stationDetectors = filteredDf.select('stationid', 'detectorid').distinct().collect()
R2 = Row('stationid', 'detectorid')
df_stationDetectors = spark.createDataFrame([R2(x,y) for x, y in stationDetectors])

#Join df_stationDetectors with df_avgDetectorSpeed on detectorid to get new table
stationDetectorSpeed = df_stationDetectors.join(df_avgDetectorSpeed, ['detectorid']).collect()
R3 = Row('stationid', 'detectorid', 'speed')
df_stationDetectorSpeed = spark.createDataFrame([R3(y, x, z) for x, y, z in stationDetectorSpeed])

#Find avgStationSpeed
avgStationSpeed = df_stationDetectorSpeed.groupBy('stationid').agg(F.avg('speed')).collect()
R4 = Row('stationid', 'speed')
df_avgStationSpeed = spark.createDataFrame([R4(x,y) for x, y in avgStationSpeed])

#Find stationLengths
#Since each station should have only 1 length, we look for distinct stationid and include the length. 
stationLengths = filteredDf.select('stationid', 'length').distinct().collect()
R5 = Row('stationid', 'length')
df_stationLengths = spark.createDataFrame([R5(x,y) for x, y in stationLengths])

#Create final dataframe that shows the travel time to each station
travelTime = df_avgStationSpeed.join(df_stationLengths, ['stationid']).withColumn("Travel Time", (F.col('length')/F.col('speed'))*60).collect()
R6 = Row('stationid', 'Travel Time')
df_travelTime = spark.createDataFrame([R6(a,d) for a,b,c,d in travelTime])

#Sum Travel Time column to find total travel time
print("Travel time for 7am-9am: \n")
df_travelTime.agg(F.sum("Travel Time")).show()

'''4pm-6pm (16:00-18:00)'''
#Filter df so we work with only the data we need
afternoonFilteredDf = df_highway.filter((df_highway.starttime > "2011-10-22 16:00:00-07") 
                              & (df_highway.starttime < "2011-10-22 18:00:00-07")
                              & (df_highway.highwayname=="I-205") 
                              & (df_highway.shortdirection=='N'))

afternoonFilteredDf.cache()
#Determine avg detector speed
avgDetectorSpeeds = afternoonFilteredDf.select('detectorid', 'speed').groupBy('detectorid').agg(F.avg('speed')).collect()
#Convert from list to df
R = Row('detectorid', 'speed')
#Create df
df_avgDetectorSpeed = spark.createDataFrame([R(x,y) for x, y in avgDetectorSpeeds])

#Determine which detectors are associated with which stations
stationDetectors = afternoonFilteredDf.select('stationid', 'detectorid').distinct().collect()
R2 = Row('stationid', 'detectorid')
df_stationDetectors = spark.createDataFrame([R2(x,y) for x, y in stationDetectors])

#Join df_stationDetectors with df_avgDetectorSpeed on detectorid to get new table
stationDetectorSpeed = df_stationDetectors.join(df_avgDetectorSpeed, ['detectorid']).collect()
R3 = Row('stationid', 'detectorid', 'speed')
df_stationDetectorSpeed = spark.createDataFrame([R3(y, x, z) for x, y, z in stationDetectorSpeed])

#Find avgStationSpeed
avgStationSpeed = df_stationDetectorSpeed.groupBy('stationid').agg(F.avg('speed')).collect()
R4 = Row('stationid', 'speed')
df_avgStationSpeed = spark.createDataFrame([R4(x,y) for x, y in avgStationSpeed])

#Find stationLengths
#Since each station should have only 1 length, we look for distinct stationid and include the length. 
stationLengths = afternoonFilteredDf.select('stationid', 'length').distinct().collect()
R5 = Row('stationid', 'length')
df_stationLengths = spark.createDataFrame([R5(x,y) for x, y in stationLengths])

#Create final dataframe that shows the travel time to each station
travelTime = df_avgStationSpeed.join(df_stationLengths, ['stationid']).withColumn("Travel Time", (F.col('length')/F.col('speed'))*60).collect()
R6 = Row('stationid', 'Travel Time')
df_travelTime = spark.createDataFrame([R6(a,d) for a,b,c,d in travelTime])

#Sum Travel Time column to find total travel time
print("Travel time for 4pm-6pm: \n")
df_travelTime.agg(F.sum("Travel Time")).show()
