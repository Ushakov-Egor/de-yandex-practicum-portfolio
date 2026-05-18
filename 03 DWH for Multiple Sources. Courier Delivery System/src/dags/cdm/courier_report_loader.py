import json
import logging
import datetime as dt
from pathlib import Path

from lib import PgConnect

log = logging.getLogger(__name__)

# Реальный путь файла: .../src/dags/cdm/courier_report_loader.py
# parents[0]=cdm, [1]=dags, [2]=src
SQL_PATH = Path(__file__).resolve().parents[2] / 'sql_scripts' / 'cdm' / 'dml' / 'infill_dm_courier_report.sql'


class CdmCourierReportLoader:
    """Заполняет витрину cdm.dm_courier_report из dds.fct_courier_deliveries."""

    WF_KEY = "cdm_dm_courier_report_load"

    def __init__(self, pg_conn: PgConnect) -> None:
        self.pg_conn = pg_conn

    def load(self) -> None:
        with self.pg_conn.connection() as conn:
            with conn.cursor() as cur:
                sql = SQL_PATH.read_text(encoding='utf-8')
                cur.execute(sql)
                log.info("cdm.dm_courier_report: витрина заполнена успешно")

                cur.execute(
                    """
                    INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                    VALUES (%s, %s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                    """,
                    (self.WF_KEY, json.dumps({"last_load_time": str(dt.datetime.now())})),
                )
                log.info("cdm.dm_courier_report: checkpoint сохранён")
