import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
 
def main():
        date = sys.argv[1]
        base_input_path = sys.argv[2]
        base_output_path = sys.argv[3]

        conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

        # Напишите директорию чтения в общем виде
        events = sql.read.json(f"{base_input_path}/date={date}")
        # Напишите директорию записи
        events.write.partitionBy('event_type').format('parquet').save(f'{base_output_path}/date={date}')


        # # Напишите директорию записи
        # events.write.partitionBy('event.event_type')\ # добавьте партиционирование
        # .format('parquet').save(f'{base_output_path}/date={date}')
        # hdfs dfs -ls /user/edgarlaksh/data/events

if __name__ == "__main__":
        main()


