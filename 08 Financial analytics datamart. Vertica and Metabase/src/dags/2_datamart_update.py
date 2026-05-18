import config              # Необходимо для работы .py скриптов из папки /py

from airflow.decorators import dag, task
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime
from py.mart_loader import MartLoader

@dag(
	dag_id='stg_to_mart',
	start_date=datetime(2022, 10, 1),
	end_date=datetime(2022, 11, 1),
	schedule_interval='15 3 * * *',
	catchup=True,
	tags=['final_project']
)
def stg_to_mart_dag():

	# Сенсор — ждёт успешного завершения pg_to_vertica за ту же logical_date
	wait_for_stg = ExternalTaskSensor(
		task_id='wait_for_stg',
		external_dag_id='pg_to_vertica',
		execution_date_fn=lambda dt: dt,     # та же logical_date
		timeout=3600,                        # 1 час ожидания
		poke_interval=60,                    # проверка каждые 60 сек
		mode='poke'
	)

	vertica_hook = VerticaHook(vertica_conn_id ="DWH_conn")                  # Получаем информацию по соединениею с источником из Airflow для создание объекта класса Loader
	martloader = MartLoader(vertica_hook)                                    # Создание обекта класса MartLoader, в дальнейшем будет использоваться для загрузки данных в слой витрин данных 

	@task
	def load_mart_global_metrics():
		table_name = 'global_metrics'
		martloader.load_mart_global_metrics(table_name)
	
	wait_for_stg >> load_mart_global_metrics()

stg_to_mart_dag()