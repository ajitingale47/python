from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("sample").getOrCreate()

df=spark.read.format("csv").option("inferschema","true").option("header","true").load("F:///Json/emp.csv")
df1=spark.read.format("csv").option("inferschema","true").option("header","true").load("F:///Json/emp1.csv")
#df.show()
#df1.show()
#df2=df.select(df["eid"],df["ename"])
#df2.show()

#df2=df.filter(df["sal"]>60000)
#df2.show()

#df2=df.filter((df["dept"]=='IT') & (df["sal"]>60000))
#df2.show()
#df2=df.filter(df["dept"]=='IT') or (df["sal"]>60000)
#df2.show()

#df2=df.groupBy(df["dept"]).sum("sal")
#df2.show()

#df2=df.groupBy(df["dept"]).avg("sal").filter(df["dept"]=='IT')

#df2=df.filter(~df["ename"] .like ("a%"))
#df2=df.filter(~df["eid"].isin(1,2,3))
#df2=df.filter(df["eid"].isNull())
#df2=df.filter(df["eid"].isNotNull())
#df2=df.dropDuplicates(["dept"])
#df2=df.withColumn('bonus',lit(1000))
#df2=df.withColumn("bonus",when(df["dept"]=='IT',lit(100)).when(df["dept"]=="HR",lit(200)).otherwise(lit(500)))

#df2=df.withColumnRenamed("EID","eid")
#df2=df.join(df1,"did","inner")
#df2=df.join(df1,"did","left")
#df2=df.join(df1,"did","right")
#df2=df.join(df1,"did","full")
#df2=df1.crossJoin(df1)
#df2=df.join(df1,(df["did"])==(df1["did"]))


#df2=df.union(df1).distinct()

#df2=df.withColumn("rn",row_number().over(Window.partitionBy(df["dept"]).orderBy(df["sal"].desc())))
#df2=df.withColumn("dn",dense_rank().over(Window.partitionBy(df["dept"]).orderBy(df["sal"].desc())))
df2=df.withColumn("r",rank().over(Window.partitionBy(df["dept"]).orderBy(df["sal"].desc())))


df2.show(100)

