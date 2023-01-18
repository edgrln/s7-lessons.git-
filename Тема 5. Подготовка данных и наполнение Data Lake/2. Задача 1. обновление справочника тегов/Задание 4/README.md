`hdfs dfs -ls /user/edgarlaksh/data/analytics/`
`hdfs dfs -ls hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/snapshots/tags_verified/actual/date=2022-05-31/event_type=message`
`hdfs dfs -ls  /user/edgarlaksh/data/analytics/verified_tags_candidates_d5/date=2022-05-31`

    spark-submit --master local /lessons/partition_overwrite.py hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/events_test



/usr/lib/spark/bin/spark-submit  /lessons/verified_tags_candidates.py 

spark-submit --master local /lessons/verified_tags_candidates.py 2022-05-31 5 300 hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/events hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/snapshots/tags_verified/actual hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/analytics/verified_tags_candidates_d5


 

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
events = spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/analytics/candidates_d7_pyspark/")
events.orderBy(F.col('suggested_count').desc()).show(3)


import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
events = spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/analytics/candidates_d84_pyspark/")
events.orderBy(F.col('suggested_count').desc()).show(3)
