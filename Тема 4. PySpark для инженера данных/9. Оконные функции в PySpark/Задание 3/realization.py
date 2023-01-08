    # import pyspark
    # from pyspark.sql import SparkSession
    # from pyspark.sql.window  ...
    # import pyspark.sql.functions ....
    
    # spark = SparkSession.builder \
    #                     .master("yarn") \
    #                     .appName("Learning DataFrames") \
    #                     .getOrCreate()
    
    # events = spark.read.parquet("/user/data/events")
    
    # window = Window().partitionBy('event. ...').orderBy('...')
    
    # dfWithLag = events.withColumn("lag_7",F.lag("event. ... ").over(window))
    
    # dfWithLag.select(...) \
    # .filter(dfWithLag.lag_7. ...) \
    # .orderBy(...) \
    # .show(10, False) 


from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.window import Window 
import pyspark.sql.functions as F 


import findspark
findspark.init()
findspark.find()
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


spark = SparkSession.builder.config("spark.driver.cores", "2").config("spark.driver.memory", "2g").appName("My first session").getOrCreate()
events = spark.read.json("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events/")

window = Window().partitionBy('event.message_from').orderBy('date')
dfWithLag = events.withColumn("lag_7",F.lag("event.message_from", 7).over(window))
dfWithLag.select("event.message_from","date", "lag_7").filter(dfWithLag.lag_7.isNotNull()).orderBy("date").show(10, False) 

dfWithLag.show(10, False)
