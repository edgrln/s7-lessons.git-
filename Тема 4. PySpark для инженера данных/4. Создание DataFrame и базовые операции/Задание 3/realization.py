# hdfs dfs -mkdir /user/edgarlaksh/analytics/test


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
df = spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/snapshots/channels/actual")
df.show(10)

df.write\
        .option("header",True)\
        .partitionBy("channel_type")\
        .mode('overwrite')\
        .parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/analytics/test")



df_2 = spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/analytics/test")
df_2.show(10)




df_final = df.select('channel_type').distinct().show()
df_final



# SparkSession.stop(spark)