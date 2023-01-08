import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

data = [('Max', 55),
        ('Yan', 53),
        ('Dmitry', 54),
        ('Ann', 25)
        ]

columns = ['Name', 'Age']
df = spark.createDataFrame(data=data, schema=columns)

df.printSchema()
