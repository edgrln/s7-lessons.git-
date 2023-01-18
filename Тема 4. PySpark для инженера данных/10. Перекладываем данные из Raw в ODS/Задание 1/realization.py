# `hdfs dfs -mkdir /user/edgarlaksh/data/events`
hdfs dfs -ls /user/edgarlaksh/data/events


import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()


df = spark.read.json("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events")

df.write\
        .option("header",True)\
        .partitionBy("date", "event_type")\
        .mode('append')\
        .parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/events")

#---------------------
df_final = spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/events")
df_final.orderBy(F.col("event.datetime").desc()).show(10)