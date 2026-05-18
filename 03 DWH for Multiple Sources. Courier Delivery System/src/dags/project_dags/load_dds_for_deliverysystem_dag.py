import logging
import os
import sys
import pendulum

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

# Добавляем src/dags в sys.path для импорта модулей dds.*
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib import ConnectionBuilder
from dds.couriers_loader import DdsCouriersLoader
from dds.delivery_addresses_loader import DdsDeliveryAddressesLoader
from dds.deliveries_loader import DdsDeliveriesLoader
from dds.courier_deliveries_loader import DdsCourierDeliveriesLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='*/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'delivery_system', 'project'],
    is_paused_upon_creation=True,
)
def sprint5_load_dds_delivery_system():

    @task()
    def t_load_dm_couriers():
        pg_conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        DdsCouriersLoader(pg_conn).load()

    @task()
    def t_load_dm_delivery_addresses():
        pg_conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        DdsDeliveryAddressesLoader(pg_conn).load()

    @task()
    def t_load_dm_deliveries():
        pg_conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        DdsDeliveriesLoader(pg_conn).load()

    @task()
    def t_load_fct_courier_deliveries():
        pg_conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        DdsCourierDeliveriesLoader(pg_conn).load()

    # Ждём завершения load_fact_task из dms_loader_dag (заполняет dds.fct_product_sales,
    # которая нужна для расчёта delivery_sum в fct_courier_deliveries).
    wait_for_fct_product_sales = ExternalTaskSensor(
        task_id="wait_for_fct_product_sales",
        external_dag_id="sprint5_dds_dms_loader",
        external_task_id="load_fact_task",
        timeout=600,
        mode='reschedule',
    )

    # dm_couriers и dm_delivery_addresses независимы — параллельно.
    # dm_deliveries требует оба справочника — запускается после них.
    # fct_courier_deliveries ждёт dm_deliveries И fct_product_sales — запускается последней.
    couriers = t_load_dm_couriers()
    addresses = t_load_dm_delivery_addresses()
    deliveries = t_load_dm_deliveries()
    courier_deliveries = t_load_fct_courier_deliveries()

    [couriers, addresses] >> deliveries >> wait_for_fct_product_sales >> courier_deliveries


sprint5_load_dds_delivery_system = sprint5_load_dds_delivery_system()
