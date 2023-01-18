import os
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import date, datetime
 
#os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
#os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
#os.environ['JAVA_HOME']='/usr'
#os.environ['SPARK_HOME'] ='/usr/lib/spark'
#os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 
 
# задаём базовые аргументы
default_args = {
    'start_date': airflow.utils.dates.days_ago(7), # datetime(2023, 1, 12), 
    'owner': 'airflow',
}
 
# вызываем DAG
dag = DAG("example_bash_dag",
          schedule_interval='0 0 * * *',
          default_args=default_args,
          catchup=False
         )

 


# объявляем задачу с Bash-командой, которая распечатывает дату
t1 = BashOperator(
    task_id='bash_spark',
    bash_command='''
    spark-submit --master local /lessons/partition_overwrite.py hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/events hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/edgarlaksh/data/events_test
    ''',
    retries=3,
    dag=dag
)

t2 = BashOperator(task_id='sleep_2', bash_command='sleep 5', retries=3, dag=dag)
t3 = BashOperator(task_id='print_success', bash_command='echo "SUCCESS"',dag=dag)

t2 >> t1 >> t3
