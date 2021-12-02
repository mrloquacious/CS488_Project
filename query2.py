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

'''Query #2 '''
print("\nQuery #2, total volume for the station Foster NB for Sept 15, 2011:")
# SELECT total volume
# WHERE Foster NB on Sept. 15
volume = df_highway.select(df_highway.volume).filter((df_highway.locationtext == "Foster")
    & (df_highway.shortdirection=='N')
    & (df_highway.starttime > "2011-10-15 00:00:00-07")
    & (df_highway.starttime < "2011-10-16 00:00:00-07")).agg(F.sum(df_highway.volume))

print("Volume for Foster NB on Sept. 15, 2011:\n")
volume.show()

