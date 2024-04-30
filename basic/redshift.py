from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("sample").getOrCreate()


#read from redshift
url='jdbc:redshift://redshift-cluster-1.cge4ifqrklbp.ap-south-1.redshift.amazonaws.com:5439/dev'
df=spark.read.format("jdbc").option("url",url).option("user","awsuser").option("password","Awspassword1").option("dbtable","users")\
    .option("driver","com.amazon.redshift.Driver").load()


df.show()



