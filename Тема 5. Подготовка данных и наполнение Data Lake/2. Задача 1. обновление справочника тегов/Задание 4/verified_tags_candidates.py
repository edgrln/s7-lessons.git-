# `hdfs dfs -mkdir /user/edgarlaksh/data/events_test
import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import datetime

date = sys.argv[1]
depth = int(sys.argv[2])
threshold = sys.argv[3]
base_input_path_1 = sys.argv[4] # hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/events

base_input_path_2 = sys.argv[5] # hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/snapshots/tags_verified/actual
base_output_path = sys.argv[6] # 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/analytics/candidates_d84_pyspark



def input_paths(date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{base_input_path_1}/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}/event_type=message" for x in range(depth)]

def main():

        spark = SparkSession.builder.master("local").appName(f"VerifiedTagsCandidatesJob-{date}-d{depth}-cut{threshold}").getOrCreate()
        paths = input_paths(date, depth)

        # Напишите директорию чтения в общем виде
        messages = spark.read.parquet(*paths)
        all_tags = messages.where("event.message_channel_to is not null").selectExpr(["event.message_from as user", "explode(event.tags) as tag"]).groupBy("tag").agg(F.expr("count(distinct user) as suggested_count")).where(f"suggested_count >= {threshold}") 
        verified_tags = spark.read.parquet(base_input_path_2)
        candidates = all_tags.join(verified_tags, "tag", "left_anti")

        # candidates.write.parquet(base_output_path)
        candidates.write.format('parquet').save(f'{base_output_path}/date={date}')
        
       

if __name__ == "__main__":
        main()


