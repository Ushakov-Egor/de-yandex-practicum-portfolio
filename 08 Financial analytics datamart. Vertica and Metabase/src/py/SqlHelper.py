from datetime import datetime
from pathlib import Path
from airflow.providers.vertica.hooks.vertica import VerticaHook

# Класс для чтения скриптов .sql и записи логов в таблтицу логов в DWH
class SqlHelper:
    def __init__(self, conn_hook: VerticaHook):
        self._conn = conn_hook

    # Метод для чтения скриптов .sql
    def load_sql(self, filename: str) -> str:
        sql_dir = Path(__file__).parent.parent / "sql"
        with open(sql_dir / filename, "r") as f:
            return f.read()
        
    # Метод для записи логов в таблтицу логов в STG
    def write_stg_log(self, schema: str, log_table_name: str, table_name: str, 
                  load_date: datetime, load_end: datetime, rows_loaded: int, 
                  status: str, error_message: str) -> None:
        sql_log = self.load_sql("load_from_src/stg_log_writer.sql").format(
            table_name=f"{schema}.{log_table_name}"
        )
        with self._conn.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_log, parameters={
                    "schema_name": str(schema),
                    "table_name": str(table_name),
                    "load_date": load_date,
                    "load_end": load_end,
                    "rows_loaded": int(rows_loaded),
                    "status": str(status),
                    "error_message": error_message
                })
            conn.commit()

    # Метод для записи логов в таблтицу логов в DWH
    def write_dwh_log(self, schema: str, log_table_name: str, 
                        load_date: datetime, table_name: str, 
                        load_end: datetime, status: str, error_message: str) -> None:
        sql_log = self.load_sql("load_to_mart/dwh_log_writer.sql").format(
            table_name=f"{schema}.{log_table_name}"
        )
        with self._conn.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_log, parameters={
                    "schema_name": str(schema),
                    "table_name": str(table_name),
                    "load_date": load_date,
                    "load_end": load_end,
                    "status": str(status),
                    "error_message": error_message
                })
            conn.commit()