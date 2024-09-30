from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .master("local[1]") \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/NOAAData.precipitation") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/NOAAData.precipitation") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

df = spark.read.csv("./data.csv", header=True)
df.printSchema()
#df.show()

# Write data to MongoDB (specify both the database and collection in the option)
df.write.format("mongodb") \
    .option("database", "NOAAData") \
    .option("collection", "precipitation") \
    .mode("append") \
    .save()


dfc = df.where(col('HPCP') != 999.99)
#dfc.where(col('HPCP') == 999.99).show()

df_distinct = df.select(countDistinct('STATION_NAME'))
#df_distinct.show()

df.groupBy('STATION_NAME').count().show(58, truncate=False)