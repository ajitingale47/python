from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("Spark Program").getOrCreate()
df=spark.read.format("CSV").option("inferschema","true").option("header","true").option("delimiter",",").load("F:///Json/emp.csv")
#df.show()
#df.printSchema()

##SELECT##
#df1=df.select(df["eid"],df["ename"])

##ALIAS##
#df1=df.select(df["eid"],df["ename"],(df["sal"]*12).alias("Asal"))

##FILTER##
#df1=df.filter(df["sal"]>70000)

 ##and(&)
#df1=df.filter((df["did"]==102) & (df["sal"]>70000))
 ##or(|)
#df1=df.filter((df["did"]==102) | (df["sal"]>60000))

##GROUP BY## with agrrigate fun

#df1=df.groupBy("did").count()
#df1=df.groupBy("did").sum("sal")
#df1=df.groupBy("did").max("sal")
#df1=df.groupBy("did").min("sal")
#df1=df.groupBy("did").avg("sal")


##groupBy filter##
#df1=df.groupBy("did").count().filter(df["did"]==101)


##LIMIT##
#df1=df.limit(5)

##orderBy(ASC & DESC)
#df1=df.orderBy(df["sal"])
#df1=df.orderBy(df["sal"].desc())

##LIKE FUNction

#df1=df.filter(df["ename"].like ("%a"))
#df1=df.filter(df["ename"].like("%a%"))
#df1=df.filter(df["ename"].like("a%"))
#df1=df.filter(df["ename"].like("_a%"))

##NEgative condition(~)
#df1=df.filter(~(df["ename"].like ("%a")))
#df1=df.filter(~(df["ename"].like("%a%")))
#df1=df.filter(~(df["ename"].like("a%")))
#df1=df.filter(~(df["ename"].like("_a%")))

##iN not in

#df1=df.filter(df["eid"].isin(1,4,7))
#df1=df.filter(~(df["eid"].isin(1,4,7)))

##is,isNot

#df1=df.filter(df["did"].isNull())
#df1=df.filter(df["did"].isNotNull())

##Distinct

#df1=df.distinct()

##dropDuplicate

#df1=df.dropDuplicates(["did"]).dropDuplicates(["ename"]).orderBy(df["ename"].desc())

##withcolumn fun

#df1=df.withColumn("bonus",lit(1000))

#df1=df.withColumn("bonus",when(df["dept"]=="IT",lit(1000)).when(df["dept"]=="HR",lit(10)).otherwise(lit(500)))

#df1=df.withColumnRenamed("sal","salary")
#df1=df.withColumnRenamed("salary","sal")


#df1.show()


##dept READ
#df=spark.read.format("CSV").option("inferschema","true").option("header","true").load("f:/Json/dept.csv")
#df1.show()

##JOIN

#df2=df.join(df1,"did","inner")

#df2=df.join(df1,"did","left")
#df2=df.join(df1,"did","right")
#df2=df.join(df1,"did","full")
#df2=df.join(df1,"did","leftanti")


##if both table join column not same
#df2=df.join(df1,df["did"]==df1["dept_did"],"inner")


#join on multiple column
#df2=df.join((df1,df["did"]==df1["dept_did"]) & (df["city"]==df1["city"]))
#df2.show()

#file 3 read
df1=spark.read.format("CSV").option("inferschema","true").option("header","true").load("f:/Json/emp1.csv")
#df2.show()

#set oparators in spark
#union work like unionall
#df2=df.union(df1)
#df2=df.union(df1).distinct()

#intersect
#df2=df.intersect(df1)

#exceptAll() means minus

#df2=df.exceptAll(df1)

#df2.show()


#windowing function
#RANK
#df1=df.withColumn("rn",rank().over(Window.partitionBy("did").orderBy("sal")))

#Row_number
#df1=df.withColumn("rn",row_number().over(Window.partitionBy("did").orderBy("sal")))

#Dense_rank
#df1=df.withColumn("dR",dense_rank().over(Window.partitionBy('did').orderBy("sal")))

# 2 nd lowest salary

#df1=df.withColumn("dr",dense_rank().over(Window.partitionBy("did").orderBy("sal")))
#df2=df1.filter(df1["dr"]==2)

# 2 nd H salary
#df1=df.withColumn("dr",dense_rank().over(Window.partitionBy("did").orderBy(col("sal").desc())))
#df2=df1.filter(df1["dr"]==2)

#date_format change
#df1=df.withColumn("NF",date_format(df["doj"],"dd-mm-yyyy"))

#currentdate
#df1=df.withColumn("today",current_date())

#current_timestamp()
#df1=df.withColumn("Time Stamp",current_timestamp())

#datediff
#df1=df.withColumn("workdur",datediff(lit(current_date()),df["doj"]))

#date_add(days)

#df1=df.withColumn("adddate",date_add(df["doj"],7))

#date_sub
#df1=df.withColumn("subdate",date_sub(df["doj"],7))

#add_months (add & sub)
#df1=df.withColumn("addmonth",add_months(df["doj"],7))
#df1=df.withColumn("submonth",add_months(df["doj"],-7))

#year,month,dayofweek,dayofmonth
#df1=df.withColumn("y",year(df["doj"]))
#df1=df.withColumn("y",month(df["doj"]))
#df1=df.withColumn("y",dayofweek(df["doj"]))
#df1=df.withColumn("y",dayofmonth(df["doj"]))

#time
#df1=df.withColumn("time",hour(df["doj"]))
#df1=df.withColumn("time",minute(df["doj"]))

#dropnullvalues
#df1=df.dropna()

#fillna
#df1=df.fillna("NA")



#df1.show(30,False)
df1.printSchema()

