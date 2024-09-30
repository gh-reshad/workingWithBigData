from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession \
    .builder \
    .master("local[1]") \
    .appName("myApp") \
    .getOrCreate()

print("Spark Session created successfully")

# Read a CSV dataset
df = spark.read.csv("./data.csv", header=True)
df.show()

