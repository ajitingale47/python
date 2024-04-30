from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Email Processing").getOrCreate()

# Load your data into a DataFrame
df = spark.read.format('csv') \
    .option('inferSchema', 'true') \
    .option('header', 'true') \
    .option('delimiter', '|') \
    .load('f:/a.csv')

# Collect the DataFrame rows into a list
rows = df.collect()

# Convert rows into a list of lists
list_of_lists = [list(row) for row in rows]

# Convert each list to a tuple and add to a set
unique_tuples_set = {tuple(row) for row in list_of_lists}

# Display the set of tuples
for item in unique_tuples_set:
    print(item)
