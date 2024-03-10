from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Without header").getOrCreate()

df=spark.read.format("CSV").option("header","true").\
    option("inferSchema","true").load("F:/emp.csv")
df.show()
df1=spark.read.format("CSV").option("header","true").\
    option("inferSchema","true").load("E:/dep.csv")
df1.show()

df.createOrReplaceTempView("emp")
df1.createOrReplaceTempView("dep")

df_res=spark.sql("""
    SELECT /*+ BROADCAST(dep) */ 
        emp.eid, emp.ename, emp.did, emp.sal
    FROM 
        emp
    INNER JOIN 
        dep ON emp.did = dep.did
""")

df_res.show()