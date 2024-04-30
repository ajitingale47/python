from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, trim, array


# Initialize a Spark session
spark = SparkSession.builder.appName("Email Processing").getOrCreate()

# Load your data into a DataFrame
df = spark.read.format('csv') \
    .option('inferSchema', 'true') \
    .option('header', 'true') \
    .option('delimiter', '|') \
    .load('f:/a.csv')

#df.show()

rows = df.collect()

# Convert rows into a list of lists
list_of_lists = [list(row) for row in rows]

# Display the list of lists
for row in list_of_lists:
    print(row)