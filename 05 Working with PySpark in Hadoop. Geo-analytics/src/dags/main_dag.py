from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Все пути и параметры в одном месте — меняем здесь, не трогая таски
SCRIPTS_PATH = '/lessons/scripts'
EVENTS_SOURCE_PATH = '/user/master/data/geo/events'
GEO_CSV_PATH = '/user/ushakovego/data/de-project-sprint-7/geo.csv'
STAGING_PATH = '/user/ushakovego/data/de-project-sprint-7/staging/events_with_city'
USERS_MART_PATH = '/user/ushakovego/data/de-project-sprint-7/analytics/users_mart'
ZONES_MART_PATH = '/user/ushakovego/data/de-project-sprint-7/analytics/zones_mart'
FRIENDS_MART_PATH = '/user/ushakovego/data/de-project-sprint-7/analytics/friends_mart'
DEPTH = '15'          # глубина выборки в днях — передаётся во все таски
DATE = '2022-06-21'   # дата актуальных данных в HDFS (заменить на {{ ds }} при работе с реальными данными)

SPARK_CONF = {
    "spark.executor.memory": "9g",
    "spark.executor.cores": "3",
    "spark.executor.instances": "5",
    "spark.driver.memory": "4g",
    "spark.driver.cores": "2",
    "spark.dynamicAllocation.enabled": "false",
    "spark.yarn.am.cores": "1",
    "spark.yarn.am.memory": "1g",
    "spark.driver.maxResultSize": "2g",
    "spark.sql.shuffle.partitions": "15",
}

with DAG(
    dag_id='geo_analytics_pipeline',
    description='Пайплайн построения витрин геоаналитики соцсети',
    start_date=datetime(2022, 6, 21),
    schedule_interval='0 0 * * *',  # ежедневно в полночь
    catchup=False,
    tags=['geo', 'analytics'],
) as dag:

    # Шаг 1: определяем город для каждого события через Haversine
    # Все последующие витрины читают результат этого шага
    events_with_city = SparkSubmitOperator(
        task_id='events_with_city',
        application=f'{SCRIPTS_PATH}/events_with_city.py',
        application_args=[DATE, DEPTH, EVENTS_SOURCE_PATH, GEO_CSV_PATH, STAGING_PATH],
        conf=SPARK_CONF,
        conn_id='yarn_spark',
    )

    # Шаг 2: витрина по пользователям (act_city, home_city, travel, local_time)
    users_mart = SparkSubmitOperator(
        task_id='users_mart',
        application=f'{SCRIPTS_PATH}/users_mart.py',
        application_args=[DATE, DEPTH, STAGING_PATH, USERS_MART_PATH],
        conf=SPARK_CONF,
        conn_id='yarn_spark',
    )

    # Шаг 3: витрина по зонам (события по городам за неделю и месяц)
    zones_mart = SparkSubmitOperator(
        task_id='zones_mart',
        application=f'{SCRIPTS_PATH}/zones_mart.py',
        application_args=[DATE, DEPTH, STAGING_PATH, ZONES_MART_PATH],
        conf=SPARK_CONF,
        conn_id='yarn_spark',
    )

    # Шаг 4: витрина рекомендации друзей
    friends_mart = SparkSubmitOperator(
        task_id='friends_mart',
        application=f'{SCRIPTS_PATH}/friends_mart.py',
        application_args=[DATE, DEPTH, STAGING_PATH, FRIENDS_MART_PATH],
        conf=SPARK_CONF,
        conn_id='yarn_spark',
    )

    # Последовательное выполнение — ресурсы кластера ограничены
    events_with_city >> users_mart >> zones_mart >> friends_mart
