import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName('Data_Wrangling').getOrCreate()
sc = spark.sparkContext
sql = SQLContext(sc)

# Create dataframes
df_highway = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         # Get proper filename
         .load("processed_data_part-00000-12d30b96-2f98-4954-b4ac-66b549f6bdcc-c000.csv")
         )


'''Query #2 '''
volume = df_highway.filter((df_highway.locationtext == "Foster") & (df_highway.shortDirection=='N')).agg(F.sum(df_highway.volume)).collect()

print(volume)


 '''Query #5'''
#avgSpeeds = avg speed per detector
#Filter by critera
#Group By: detectorId and sum their speeds. Returns a list
avgSpeeds = df_highway.filter((df_highway.starttime > "2011-10-11 07:00:00-07") 
                              & (df_highway.starttime < "2011-10-11 09:00:00-07")
                              & (df_highway.highwayname=="I-205") 
                              & (df_highway.shortdirection=='N')).groupBy(df_highway.detectorid).agg(F.avg(df_highway.speed).alias('avgSpeed')).collect()

#Find length by detector
#Each unique detector has a length, so we want (detectorId, length) together.
#Filter by critera
lengths = df_highway.filter((df_highway.starttime > "2011-10-11 07:00:00-07") 
                              & (df_highway.starttime < "2011-10-11 09:00:00-07")
                              & (df_highway.highwayname=="I-205") 
                              & (df_highway.shortdirection=='N')).select(df_highway.detectorid,df_highway.length).distinct().collect()

#Convert lists to Dataframe
#totalSpeeds
R = Row('stationId', 'speed',)
totalSpeeds = spark.createDataFrame([R(x,y) for x, y in avgSpeeds])

#detectorLengths
R2 = Row('stationId', 'length',)
detectorLengths = spark.createDataFrame([R2(x,y) for x, y in lengths])
detectorLengths.show()

#Join dataframes: travelTime(detectorid, (length/avgSpeed)*60)
#find sum of travelTime
