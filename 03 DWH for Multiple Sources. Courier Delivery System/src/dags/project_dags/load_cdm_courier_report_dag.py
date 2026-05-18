import logging
import os
import sys
import pendulum

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

# Добавляем src/dags в sys.path для импорта модулей cdm.*
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib import ConnectionBuilder
from cdm.courier_report_loader import CdmCourierReportLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/20 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'delivery_system', 'project'],
    is_paused_upon_creation=True,
)
def sprint5_load_cdm_courier_report():

    # Ждём завершения cdm_report_loader из cdm_dag (заполняет cdm.dm_settlement_report).
    # Оба DAG-а запускаются по расписанию '0/20 * * * *', execution_date совпадает.
    wait_for_settlement_report = ExternalTaskSensor(
        task_id="wait_for_settlement_report",
        external_dag_id="sprint5_cdm_report_loader",
        external_task_id="cdm_report_loader",
        timeout=600,
        mode='reschedule',
    )

    @task()
    def t_load_dm_courier_report():
        pg_conn = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        CdmCourierReportLoader(pg_conn).load()

    wait_for_settlement_report >> t_load_dm_courier_report()


sprint5_load_cdm_courier_report = sprint5_load_cdm_courier_report()
