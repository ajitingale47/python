
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("spark testing").getOrCreate()

ls=[1,2,3,4]
rdd=spark.sparkContext.parallelize(ls)
print(rdd.collect())

rdd.getNumPartitions()
rdd1=rdd.repartition(4)
rdd1.cache()

from pyspark.storagelevel import StorageLevel

rdd.persist(StorageLevel.MEMORY_AND_DISK)