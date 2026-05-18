from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SCRIPTS_PATH = '/lessons/scripts'
ZONES_MART_PATH = '/user/ushakovego/data/de-project-sprint-7/analytics/zones_mart'

SPARK_CONF = {
    "spark.executor.memory": "4g",
    "spark.executor.cores": "2",
    "spark.executor.instances": "1",
    "spark.driver.memory": "2g",
    "spark.driver.cores": "1",
    "spark.yarn.am.cores": "1",
    "spark.yarn.am.memory": "1g",
    "spark.dynamicAllocation.enabled": "false",
    "spark.sql.shuffle.partitions": "15",
}

with DAG(
    dag_id='validate_zones_mart',
    description='Проверка качества данных витрины zones_mart',
    start_date=datetime(2022, 6, 21),
    schedule_interval='30 0 * * *',  # запускается через 30 минут после основного пайплайна
    catchup=False,
    tags=['validation', 'analytics'],
) as dag:

    # Проверки zones_mart:
    # 1. Сумма недельных сообщений == месячные
    # 2. Регистрации не превышают сообщения за неделю
    # 3. Нет null в ключевых полях (падает с ошибкой если есть)
    validate_zones = SparkSubmitOperator(
        task_id='validate_zones_mart',
        application=f'{SCRIPTS_PATH}/validate_zones_mart.py',
        application_args=[ZONES_MART_PATH],
        conf=SPARK_CONF,
        conn_id='yarn_spark',
    )
