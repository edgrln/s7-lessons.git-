# `hdfs dfs -mkdir /user/edgarlaksh/data/events_test
import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
 
def main():
        date = '2022-05-31' # sys.argv[1]
        base_input_path = sys.argv[1]
        base_output_path = sys.argv[2]

        # conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}")
        # sc = SparkContext(conf=conf)
        # sql = SQLContext(sc)

        spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate() 

        # Напишите директорию чтения в общем виде
        # events = sql.read.json(f"{base_input_path}/date={date}")
       # events = spark.read.json(f"{base_input_path}") #/date={date}")
        df = spark.read.json(f"{base_input_path}")
 
        # Напишите директорию записи
        # events.write.partitionBy('event_type').format('parquet').save(f'{base_output_path}') #/date={date}')
        df.write.option("header",True).partitionBy("date", "event_type").mode('append').parquet(f"{base_input_path}")

        # # Напишите директорию записи
        # events.write.partitionBy('event.event_type')\ # добавьте партиционирование
        # .format('parquet').save(f'{base_output_path}/date={date}')
        # hdfs dfs -ls /user/edgarlaksh/data/events

if __name__ == "__main__":
        main()


