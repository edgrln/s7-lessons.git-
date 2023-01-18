import datetime
import pyspark.sql.functions as F
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

def input_paths(date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(depth)]

paths = input_paths('2022-05-31', 84)
messages = spark.read.parquet(*paths)
all_tags = messages.where("event.message_channel_to is not null").selectExpr(["event.message_from as user", "explode(event.tags) as tag"]).groupBy("tag").agg(F.expr("count(distinct user) as suggested_count")).where("suggested_count >= 100") 
verified_tags = spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/snapshots/tags_verified/actual")
candidates = all_tags.join(verified_tags, "tag", "left_anti")

candidates.write.parquet('hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/analytics/candidates_d84_pyspark')
