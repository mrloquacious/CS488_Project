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

'''Query #1 '''
print("Query #1, count low speeds and high speeds:")
# SELECT COUNT(speed)
# WHERE speed < 5 OR speed > 80
print(df_highway.select('speed').where((df_highway.speed<5) | (df_highway.speed>80)).count())


