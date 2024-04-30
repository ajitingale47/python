from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("Spark Program").getOrCreate()

# Read the CSV file as an RDD
rdd = spark.sparkContext.textFile("file:///f:/a.csv")

# Define a function to replace delimiters with ","
def replace_delimiters(line):
    # Replace all delimiters (, ; |) with ","
    return line.replace(",", ",").replace(";", ",").replace("|", ",")

# Apply the function to each line of the RDD
rdd1 = rdd.map(replace_delimiters)

# Define schema for DataFrame
schema = Row("eid", "ename", "sal", "dept")

# Convert RDD to DataFrame
df = rdd1.map(lambda x: x.split(",")).map(lambda x: schema(*x)).toDF()

# Show the DataFrame
df.show()
