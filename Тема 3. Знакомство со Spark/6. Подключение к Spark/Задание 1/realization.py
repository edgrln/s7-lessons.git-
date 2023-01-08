# import findspark
# findspark.init()
# findspark.find()
# import os
# os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


from pyspark.sql import SparkSession
spark = SparkSession.builder.config("spark.driver.cores", "2").config("spark.driver.memory", "1g").appName("My first session").getOrCreate()