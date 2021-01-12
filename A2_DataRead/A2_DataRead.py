# Databricks notebook source
# Create Sparksession 

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

# COMMAND ----------

# /FileStore/tables/data/제주특별자치도_서귀포시_고령화비율및노령화지수현황_20200623.csv
agedRdd = spark.sparkContext\
    .textFile(os.path.join("/FileStore/tables/data","제주특별자치도_서귀포시_고령화비율및노령화지수현황_20200623.csv"), use_unicode=True)

# COMMAND ----------

agedRdd.take(5)

# COMMAND ----------

for i in agedRdd.take(5):
    print(i)

# COMMAND ----------

ageRddBin = spark.sparkContext.binaryFiles(os.path.join("/FileStore/tables/data","제주특별자치도_서귀포시_고령화비율및노령화지수현황_20200623.csv"))

# COMMAND ----------

_my = ageRddBin.map(lambda x :x[1].decode('euc-kr'))

# COMMAND ----------

_my.take(1)

# COMMAND ----------

ageList = _my.map(lambda x: x.split()).take(3)
ageList[0][1]

# COMMAND ----------

agedDf = spark\
            .read.option("charset", "euc-kr")\
            .option("header", "true")\
            .csv(os.path.join("/FileStore/tables/data","제주특별자치도_서귀포시_고령화비율및노령화지수현황_20200623.csv"))

# COMMAND ----------

agedDf.show(5)

# COMMAND ----------

import os
import pyspark

def doIt():
    print ("---------RESULT-----------")
    agedDf = spark\
                .read.option("charset", "euc-kr")\
                .option("header", "true")\
                .csv(os.path.join("/FileStore/tables/data","제주특별자치도_서귀포시_고령화비율및노령화지수현황_20200623.csv"))
    agedDf.show(5)

if __name__ == "__main__":
    os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/bin/python3"
    myConf=pyspark.SparkConf()
    spark = pyspark.sql.SparkSession.builder\
        .master("local")\
        .appName("myApp")\
        .config(conf=myConf)\
        .getOrCreate()
    doIt()
    spark.stop()

# COMMAND ----------

!spark-submit src/ds3_popCsvRead.py
