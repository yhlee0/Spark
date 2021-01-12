# Databricks notebook source
import os
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3"

# COMMAND ----------

# Create a RDD from the list.(Create list)
myList=[1,2,3,4,5,6,7]

# COMMAND ----------

# Create a RDD from the list.(Create RDD)
myRdd1 = spark.sparkContext.parallelize(myList)

# COMMAND ----------

myRdd1.take(3)

# COMMAND ----------

spark.sparkContext.parallelize([0, 2, 3, 4, 6], 2).collect()

# COMMAND ----------

spark.sparkContext.parallelize([1,2,3,4,5,6,7], 2).glom().collect()

# COMMAND ----------

# "%%writefile data/ds_spark_wiki.txt" in jupyter notebook.
# fileupload or fs.put() in databricks

jstxt="""
Wikipedia
Apache Spark is an open source cluster computing framework.
아파치 스파크는 오픈 소스 클러스터 컴퓨팅 프레임워크이다.
Apache Spark Apache Spark Apache Spark Apache Spark
아파치 스파크 아파치 스파크 아파치 스파크 아파치 스파크
Originally developed at the University of California, Berkeley's AMPLab,
the Spark codebase was later donated to the Apache Software Foundation,
which has maintained it since.
Spark provides an interface for programming entire clusters with
implicit data parallelism and fault-tolerance."""
dbutils.fs.put("/data/ds_spark_wifi2.txt", jstxt)

# COMMAND ----------

# Create RDD from the file.
myRdd2=spark.sparkContext\
    .textFile(os.path.join("/FileStore/tables/data/","ds_spark_wiki.txt"))

# COMMAND ----------

myRdd2.first()

# COMMAND ----------

import os
myDf=spark.read.text(os.path.join("/FileStore/tables/data/", "ds_spark_wiki.txt"))
print (myDf.first())

# COMMAND ----------

print (type(myDf))

# COMMAND ----------

# fs.put() file
# %%writefile ds_spark_2cols.csv in jupyter notebook.
jstxt="""
35, 2
40, 27
12, 38
15, 31
21, 1
14, 19
46, 1
10, 34
28, 3
48, 1
16, 2
30, 3
32, 2
48, 1
31, 2
22, 1
12, 3
39, 29
19, 37
25, 2"""
dbutils.fs.put("/FileStore/tables/data/ds_spark_2cols.csv", jstxt)

# COMMAND ----------

# Create dataframe from the file.
myRdd4 = spark.sparkContext\
    .textFile(os.path.join("/FileStore/tables/data/","ds_spark_2cols.csv"))

# COMMAND ----------

myList=myRdd4.take(5)
print (type(myList))
