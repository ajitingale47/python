from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Email Processing").getOrCreate()

# Load your data into a DataFrame
df = spark.read.format('csv') \
    .option('inferSchema', 'true') \
    .option('header', 'true') \
    .option('delimiter', '|') \
    .load('f:/a.csv')
df.show()
df.fillna("").show()
# Collect the DataFrame rows into a list
rows = df.collect()

# Convert the table into a list of lists
converted_list = []
unique_items = set()
for row in rows:
    converted_row = []
    for item in row:
        if item is not None and item not in unique_items:
            converted_row.append(item)
            unique_items.add(item)
        elif item is None:
            converted_row.append
    converted_list.append(converted_row)

# Display the converted list
for row in converted_list:
    print(row)