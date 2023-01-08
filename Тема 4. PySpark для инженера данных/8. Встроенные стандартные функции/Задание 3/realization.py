# from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

# spark = SparkSession.builder.config("spark.driver.cores", "2").config("spark.driver.memory", "1g").appName("My first session").getOrCreate()

# events = spark.read.json("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events/")
# spark.close()


event_from=events.filter(F.col('event_type')=='reaction').groupBy(F.col('date')).count()
event_from.orderBy(F.col('count').desc()).show() 