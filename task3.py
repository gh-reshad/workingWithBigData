from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col

spark = SparkSession \
    .builder \
    .master("local[1]") \
    .appName("myApp") \
    .getOrCreate()

df = spark.read.csv("./data.csv", header=True)
df.printSchema()
#df.show()

dfc = df.where(col('HPCP') != 999.99)
#dfc.where(col('HPCP') == 999.99).show()

df_distinct = df.select(countDistinct('STATION_NAME'))
#df_distinct.show()

df.groupBy('STATION_NAME').count().show(58, truncate=False)


