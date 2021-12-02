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
#avgSpeeds = avg speed per detector
#Filter by critera
#Group By: detectorId and sum their speeds. Returns a list
avgSpeeds = df_highway.filter((df_highway.starttime > "2011-10-22 07:00:00-07") 
                              & (df_highway.starttime < "2011-10-22 09:00:00-07")
                              & (df_highway.highwayname=="I-205") 
                              & (df_highway.shortdirection=='N')).groupBy(df_highway.detectorid).agg(F.avg(df_highway.speed).alias('avgSpeed')).collect()

# Convert list into dataframe
R = Row('detectorid', 'speed',)
totalSpeeds = spark.createDataFrame([R(x,y) for x, y in avgSpeeds])

#determin lengths for detectors
lengths = df_highway.filter((df_highway.starttime > "2011-10-22 07:00:00-07") 
                              & (df_highway.starttime < "2011-10-22 09:00:00-07")
                              & (df_highway.highwayname=="I-205") 
                              & (df_highway.shortdirection=='N')).select(df_highway.detectorid,df_highway.length).distinct().collect()

#Conver list into dataframe
R2 = Row('detectorid', 'length',)
detectorLengths = spark.createDataFrame([R2(x,y) for x, y in lengths])

# Join based on detectorid and create a new column called "Travel Time" that is detector length/detector speed
totalSpeeds = totalSpeeds.join(detectorLengths, ['detectorid']).withColumn("Travel Time", (F.col('length')/F.col('speed')))
print("Travel time for I-205 NB on Sep. 22 from 7am-9am\n")
totalSpeeds.agg(F.sum("Travel Time")).show()

'''4pm-6pm'''
# Same stuff but for different time
avgSpeeds = df_highway.filter((df_highway.starttime > "2011-10-22 16:00:00-07") 
                              & (df_highway.starttime < "2011-10-22 18:00:00-07")
                              & (df_highway.highwayname=="I-205") 
                              & (df_highway.shortdirection=='N')).groupBy(df_highway.detectorid).agg(F.avg(df_highway.speed).alias('avgSpeed')).collect()

R = Row('detectorid', 'speed',)
totalSpeeds = spark.createDataFrame([R(x,y) for x, y in avgSpeeds])

lengths = df_highway.filter((df_highway.starttime > "2011-10-22 16:00:00-07") 
                              & (df_highway.starttime < "2011-10-22 18:00:00-07")
                              & (df_highway.highwayname=="I-205") 
                              & (df_highway.shortdirection=='N')).select(df_highway.detectorid,df_highway.length).distinct().collect()

R2 = Row('detectorid', 'length',)
detectorLengths = spark.createDataFrame([R2(x,y) for x, y in lengths])

totalSpeeds = totalSpeeds.join(detectorLengths, ['detectorid']).withColumn("Travel Time", (F.col('length')/F.col('speed')))
print("Travel time for I-205 NB on Sep. 22 from 4pm-6pm\n")
totalSpeeds.agg(F.sum("Travel Time")).show()
