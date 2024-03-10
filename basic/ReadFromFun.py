from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from Functions import *

spark=SparkSession.builder.appName("sample").getOrCreate()

df_emp=read_csv(spark,'F:/emp.csv')
df_dep=read_csv(spark,'E:/dep.csv')
df_data=read_csv(spark,"F:/Json/sample.json")
df_data1=read_csv(spark,"F:/Json/sample_mul.json")
df_emp.show()
df_dep.show()
df_data.show()
df_data1.show()