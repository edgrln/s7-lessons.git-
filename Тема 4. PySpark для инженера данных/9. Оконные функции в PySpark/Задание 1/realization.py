import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
# данные  датафрейма 
data = [('2021-01-06', 3744, 63, 322),
        ('2021-01-04', 2434, 21, 382),
        ('2021-01-04', 2434, 32, 159),
        ('2021-01-05', 3744, 32, 159),
        ('2021-01-06', 4342, 32, 159),
        ('2021-01-05', 4342, 12, 259),
        ('2021-01-06', 5677, 12, 259),
        ('2021-01-04', 5677, 23, 499)
]
# названия атрибутов
columns = ['dt', 'user_id', 'product_id', 'purchase_amount']
# создаём датафрейм
df = spark.createDataFrame(data=data, schema=columns)


# импортируем оконную функцию и модуль Spark Functions
from pyspark.sql.window import Window 
import pyspark.sql.functions as F

# создаём объект оконной функции
# window = Window().partitionBy(['dt']).orderBy(F.asc('purchase_amount'))

window = Window().orderBy(F.asc('purchase_amount'))

# создаём колонку с рассчитанной статистикой по оконной функции
df_window = df.withColumn("row_number", F.row_number().over(window))

# выводим нужные колонки
df_window.select('dt', 'user_id', 'purchase_amount', 'row_number').show()


