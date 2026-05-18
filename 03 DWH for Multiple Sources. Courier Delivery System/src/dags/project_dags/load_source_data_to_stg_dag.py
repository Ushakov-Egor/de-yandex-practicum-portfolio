import logging
import os
import sys
import psycopg2
import pendulum

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

# Добавляем src/dags в sys.path, чтобы Airflow мог найти модули stg.*
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from stg.couriers_loader import CouriersLoader
from stg.deliveries_loader import DeliveriesLoader

log = logging.getLogger(__name__)


def get_pg_connection():
    """Создаёт подключение к PostgreSQL через Airflow Connection."""
    conn_info = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
    return psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )


@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'delivery_system'],
    is_paused_upon_creation=True,
)
def sprint5_project_load_stg():

    @task()
    def t_load_couriers():
        pg_conn = get_pg_connection()
        loader = CouriersLoader(pg_conn)
        loader.load_couriers()

    @task()
    def t_load_deliveries():
        pg_conn = get_pg_connection()
        loader = DeliveriesLoader(pg_conn)
        loader.load_deliveries()

    # Курьеры загружаются перед доставками (доставки ссылаются на courier_id)
    t_load_couriers() >> t_load_deliveries()


sprint5_project_load_stg = sprint5_project_load_stg()
