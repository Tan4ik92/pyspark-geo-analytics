from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'tanyaganiy',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='geo_datamarts_daily',
    default_args=default_args,
    description='Ежедневный расчёт витрин пользователей, зон и рекомендаций друзей',
    schedule='0 3 * * *',              # каждый день в 03:00 утра
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['geo', 'spark', 'datamart', 'daily'],
    max_active_runs=1,
) as dag:

    # Задача 1: витрина пользователей
    users_datamart = SparkSubmitOperator(
        task_id='build_users_datamart',
        name='Users Datamart - Spark Job',
        application='/lessons/users_datamart.py',  
        conn_id='spark_default',                    
        conf={
            'spark.master': 'yarn',
            'spark.submit.deployMode': 'cluster', 
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.executor.instances': '10',
            'spark.dynamicAllocation.enabled': 'true',
            'spark.dynamicAllocation.minExecutors': '5',
            'spark.dynamicAllocation.maxExecutors': '20',
        }
    )

    # Задача 2: витрина по зонам
    zones_datamart = SparkSubmitOperator(
        task_id='build_zones_datamart',
        name='Zones Datamart - Spark Job',
        application='/lessons/zones_datamart.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'yarn',
            'spark.submit.deployMode': 'cluster',
            'spark.executor.memory': '6g',
            'spark.executor.cores': '3',
            'spark.executor.instances': '12',
        },
    )

    # Задача 3: рекомендации друзей
    friends_recommendation = SparkSubmitOperator(
        task_id='build_friends_recommendation',
        name='Friends Recommendation - Spark Job',
        application='/lessons/friends_recommendation.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'yarn',
            'spark.submit.deployMode': 'cluster',
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.executor.instances': '8',
        },
    )

    # Зависимости: users → zones → friends
    users_datamart >> zones_datamart >> friends_recommendation