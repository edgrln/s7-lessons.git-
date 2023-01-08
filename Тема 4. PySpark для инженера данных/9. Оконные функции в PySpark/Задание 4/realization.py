+-------+---+---+
|user_id|max|min|
+-------+---+---+
|   2434|382|159|
|   4342|259|159|
|   5677|499|259|
|   3744|322|159|
+-------+---+---+


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window 
import pyspark.sql.functions as F

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

data = [('2021-01-06', 3744, 63, 322),
        ('2021-01-04', 2434, 21, 382),
        ('2021-01-04', 2434, 32, 159),
        ('2021-01-05', 3744, 32, 159),
        ('2021-01-06', 4342, 32, 159),
        ('2021-01-05', 4342, 12, 259),
        ('2021-01-06', 5677, 12, 259),
        ('2021-01-04', 5677, 23, 499)
]

columns = ['dt', 'user_id', 'product_id', 'purchase_amount']

df = spark.createDataFrame(data=data, schema=columns)

df.show(10, False)

window = Window().partitionBy('user_id')

df_window_agg= df.withColumn("max",F.max("purchase_amount").over(window)).withColumn("min",F.min("purchase_amount").over(window)).select('user_id', 'min', 'max')
df_window_agg.show()