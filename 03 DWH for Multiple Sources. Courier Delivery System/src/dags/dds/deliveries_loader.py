import json
import logging
import datetime as dt
from pathlib import Path

from lib import PgConnect

log = logging.getLogger(__name__)

SQL_PATH = Path(__file__).resolve().parents[2] / 'sql_scripts' / 'dds' / 'dml' / 'infill_dm_deliveries.sql'


class DdsDeliveriesLoader:
    """Загружает доставки из STG → dds.dm_deliveries.
    Зависит от: dds.dm_orders, dds.dm_delivery_addresses, dds.dm_couriers.
    """

    WF_KEY = "dds_dm_deliveries_load"

    def __init__(self, pg_conn: PgConnect) -> None:
        self.pg_conn = pg_conn

    def load(self) -> None:
        with self.pg_conn.connection() as conn:
            with conn.cursor() as cur:
                sql = SQL_PATH.read_text(encoding='utf-8')
                cur.execute(sql)
                log.info("dds.dm_deliveries: данные из STG загружены успешно")

                cur.execute(
                    """
                    INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
                    VALUES (%s, %s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                    """,
                    (self.WF_KEY, json.dumps({"last_load_time": str(dt.datetime.now())})),
                )
                log.info("dds.dm_deliveries: checkpoint сохранён")
