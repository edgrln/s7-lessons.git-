import datetime
import pyspark.sql.functions as F
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()


df = spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/analytics/candidates_d7_pyspark")
df.describe().show()


######
######


import datetime
import pyspark.sql.functions as F
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()


df = spark.read.parquet("hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/analytics/candidates_d84_pyspark")
df.describe().show()