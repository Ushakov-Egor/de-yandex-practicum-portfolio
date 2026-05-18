import json
import logging
import datetime as dt
from pathlib import Path

from lib import PgConnect

log = logging.getLogger(__name__)

SQL_PATH = Path(__file__).resolve().parents[2] / 'sql_scripts' / 'dds' / 'dml' / 'infill_dm_couriers.sql'


class DdsCouriersLoader:
    """Загружает курьеров из STG → DDS, выполняя SQL-скрипт."""

    WF_KEY = "dds_dm_couriers_load"

    def __init__(self, pg_conn: PgConnect) -> None:
        self.pg_conn = pg_conn

    def load(self) -> None:
        # pg_conn.connection() — контекстный менеджер из lib/pg_connect.py:
        # при успехе делает commit, при исключении — rollback, затем закрывает соединение.
        with self.pg_conn.connection() as conn:
            with conn.cursor() as cur:

                sql = SQL_PATH.read_text(encoding='utf-8')
                cur.execute(sql)
                log.info("dds.dm_couriers: данные из STG загружены успешно")

                # Сохраняем факт запуска в wf_settings
                cur.execute(
                    """
                    INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                    VALUES (%s, %s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                    """,
                    (self.WF_KEY, json.dumps({"last_load_time": str(dt.datetime.now())})),
                )
                log.info(f"dds.dm_couriers: checkpoint сохранён")
