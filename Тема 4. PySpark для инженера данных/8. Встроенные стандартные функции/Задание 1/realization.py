import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
events = spark.read.json("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events/")
events.show(10)


import pyspark.sql.functions as F 
events_hour_min_sec = events.withColumn('hour',F.hour(F.col('event.datetime'))).withColumn('minute',F.minute(F.col('event.datetime'))).withColumn('second',F.second(F.col('event.datetime'))).orderBy(F.col('event.datetime').desc())
events_hour_min_sec.show(10, False)