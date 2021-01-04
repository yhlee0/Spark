# Databricks notebook source
popList = _my.map(lambda x: x.split()).take(3)
popList[0][0]

# COMMAND ----------

popList = _my.map(lambda x: x.split()).take(3)
popList[0]

# COMMAND ----------

popList = _my.map(lambda x: x.split()).take(3)
popList[0][1]

# COMMAND ----------

_my.take(1)

# COMMAND ----------

_my = popRddBin.map(lambda x :x[1].decode('euc-kr'))

# COMMAND ----------
# Read by binaryFiles()
popRddBin = spark.sparkContext.binaryFiles(os.path.join("/FileStore/tables/data","경기도_의정부시_인구현황_20201202.csv"))

# COMMAND ----------
# Read by DataFrame
popDf = spark\
            .read.option("charset", "euc-kr")\
            .option("header", "true")\
            .csv(os.path.join("/FileStore/tables/data","경기도_의정부시_인구현황_20201202.csv"))

popDf.show(5)

# COMMAND ----------

for i in popRdd.take(5):
    print(i)

# COMMAND ----------

popRdd.take(5)

# COMMAND ----------
# Read by Rdd(1)
popRdd = spark.sparkContext\
    .textFile(os.path.join("/FileStore/tables/data","경기도_의정부시_인구현황_20201202.csv"), use_unicode=True)

# COMMAND ----------
# CreateSparkSession
import os
import pyspark

# os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
# os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3"
myConf=pyspark.SparkConf()
spark = pyspark.sql.SparkSession\
    .builder\
    .master("local")\
    .appName("myApp")\
    .config(conf=myConf)\
    .getOrCreate()
