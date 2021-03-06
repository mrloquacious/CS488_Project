import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sql = SQLContext(sc)

# Create dataframes
df_highway = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("gs://cs488-project--flat-atlas-bucket/freeway_data/processed_data.csv")
         )

df_highway.cache()

'''Query #5'''

print("\nQuery #5, average travel time for 7-9AM and 4-6PM on September 22, 2011 for the I-205 NB freeway:")
'''7am-9am'''
#Filter df so we work with only the data we need
filteredDf = df_highway.filter((df_highway.starttime > "2011-10-22 07:00:00-07") 
                              & (df_highway.starttime < "2011-10-22 09:00:00-07")
                              & (df_highway.highwayname=="I-205") 
                              & (df_highway.shortdirection=='N')
                              & (df_highway.speed > 0))

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
                              & (df_highway.shortdirection=='N')
                              & (df_highway.speed > 0))


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
