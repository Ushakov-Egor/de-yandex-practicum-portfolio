from airflow.models import Variable
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.python import get_current_context

from datetime import datetime, date, timedelta
import logging
from py.SqlHelper import SqlHelper

class MartLoader:
	def __init__(self, vertica: VerticaHook) -> None:
		# Хук соединения c Vertica из Airflow
		self._dwh = vertica
		self.cdm_schema = Variable.get('cdm_schema')        # Схема слоя витрин в DWH
		self.stg_schema = Variable.get('stg_schema')        # Схема стейнджинг-слоя в DWH
		# Создание объекта SqlHelper
		self.sqlhelper = SqlHelper(self._dwh)
		# Создание логгера
		self.logger = logging.getLogger(__name__)
		# Переменные с именами таблиц в слое STG
		self.t_currencies = 'currencies'
		self.t_transactions = 'transactions'

	# Метод получения даты за которую нужно загрузить данные из STG в витрину
	def _get_load_date(self) -> date:
		context = get_current_context()
		return context['data_interval_start'].date()

	# Метод загрузки данных из слоя STG в слой витрин
	def load_mart_global_metrics(self, table_name: str) -> None:

		self.load_date = self._get_load_date()
		self.logger.info("Интервал загрузки данных из STG: %s ", self.load_date)

		# Загрузка данных в витрину
		sql = self.sqlhelper.load_sql('load_to_mart/load_mart_global_metrics.sql').format(table_name=f"{self.cdm_schema}.{table_name}", 
																							stg_transactions=f"{self.stg_schema}.{self.t_transactions}", 
																							stg_currencies=f"{self.stg_schema}.{self.t_currencies}")
		with self._dwh.get_conn() as conn:
			with conn.cursor() as cursor:
				try:
					cursor.execute(sql, parameters =  {"load_date": str(self.load_date)})
					conn.commit()
					self.logger.info("Данные загружены в витрину %s за период: %s", table_name, self.load_date)
					
					# Запись лога в DWH
					self.logger.info("Запись лога в DWH")
					load_end = datetime.now()
					status = "SUCCESS"
					error_message = None
					self.sqlhelper.write_dwh_log(self.cdm_schema,'load_log', self.load_date, table_name, load_end, status, error_message)
				except Exception as e:
					self.logger.error("Ошибка загрузки %s : %s", table_name, e)

					# Запись лога в DWH
					self.logger.info("Запись лога в DWH")
					load_end = datetime.now()
					status = "ERROR"
					error_message = str(e)
					self.sqlhelper.write_dwh_log(self.cdm_schema,'load_log', self.load_date, table_name, load_end, status, error_message)
					raise
