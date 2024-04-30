from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("sample").getOrCreate()


df=spark.read.format("org.apache.phoenix.spark").option("ZkUrl","localHost:2181").option("table","emp").load()


df=spark.write.format("csv").option("inferschema","true").option("header","true").load("s3://ajit1/emp.csv")