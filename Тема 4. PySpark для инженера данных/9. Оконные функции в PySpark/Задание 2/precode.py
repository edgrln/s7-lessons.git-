# import pyspark
# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#                     .master("local") \
#                     .appName("Learning DataFrames") \
#                     .getOrCreate()
# events = spark.read.json("/user/master/data/events")


from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession.builder.config("spark.driver.cores", "2").config("spark.driver.memory", "3g").appName("My first session").getOrCreate()

events = spark.read.json("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events/")
events.show(10, False)

events.printSchema()
