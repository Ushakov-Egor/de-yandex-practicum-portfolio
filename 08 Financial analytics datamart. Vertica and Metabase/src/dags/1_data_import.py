import config              # Необходимо для работы .py скриптов из папки /py

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook

from datetime import datetime
from py.stg_loader import Loader

@dag(
	dag_id='pg_to_vertica',
	start_date=datetime(2022, 10, 1),
	end_date=datetime(2022, 11, 1),
	schedule_interval='0 3 * * *',
	catchup=True,
	tags=['final_project']
)
def pg_to_vertica_dag():

	pg_hook = PostgresHook(postgres_conn_id="SRC_conn")                # Получаем информацию по соединениею с источником из Airflow для создание объекта класса Loader
	vertica_hook = VerticaHook(vertica_conn_id ="DWH_conn")            # Получаем информацию по соединениею с источником из Airflow для создание объекта класса Loader                

	# Таска для загрузки таблицы transactions
	@task
	def load_transactions():
		table_name = 'transactions'                                                     # Имя таблицы
		loader_for_transactions = Loader(pg_hook, vertica_hook)                         # Создание обекта класса Loader, в дальнейшем будет использоваться для загрузки данных из источника Postgres
		df_transactions = loader_for_transactions.get_transactions_data(table_name)     # Получение датафрейма из источника
		loader_for_transactions.load_data(df_transactions, table_name)                  # Загрузка датафрейма в хранилище

	# Таска для загрузки таблицы currencies
	@task
	def load_currencies():
		table_name = 'currencies'                                                     # Имя таблицы
		loader_for_currencies = Loader(pg_hook, vertica_hook)                         # Создание обекта класса Loader, в дальнейшем будет использоваться для загрузки данных из источника Postgres
		df_currencies = loader_for_currencies.get_currencies_data(table_name)         # Получение датафрейма из источника
		loader_for_currencies.load_data(df_currencies, table_name)                    # Загрузка датафрейма в хранилище

	load_transactions() >> load_currencies()

pg_to_vertica_dag()