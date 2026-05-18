import json
import logging
import datetime as dt
from typing import Any, List

import requests
from pydantic import BaseModel

log = logging.getLogger(__name__)

API_BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
API_HEADERS = {
    'X-Nickname': 'your_nickname',       # задать через переменную окружения
    'X-Cohort': 'your_cohort_number',    # задать через переменную окружения
    'X-API-KEY': 'your_api_key',         # задать через переменную окружения
}

# По заданию: загружаем данные за последние 7 дней при отсутствии checkpoint
LOAD_DAYS_BACK = 7


class DeliveryObj(BaseModel):
    delivery_id: str
    order_id: str
    order_ts: str
    courier_id: str
    address: str
    delivery_ts: str
    rate: int
    delivery_sum: float
    tip_sum: float


class DeliveriesOriginRepository:
    """Загружает доставки из HTTP API с пагинацией и фильтром по дате."""

    BATCH_SIZE = 50
    ENDPOINT = '/deliveries'

    def list_deliveries(self, from_ts: str) -> List[DeliveryObj]:
        url = f"{API_BASE_URL}{self.ENDPOINT}"
        objs = []
        offset = 0

        while True:
            params = {
                'sort_field': '_id',
                'sort_direction': 'asc',
                'limit': self.BATCH_SIZE,
                'offset': offset,
                'from': from_ts,
            }
            rs = requests.get(url, headers=API_HEADERS, params=params)

            if rs.status_code != 200:
                raise Exception(f"API error: status_code={rs.status_code}, body={rs.text}")

            batch = json.loads(rs.content)
            log.info(f"offset={offset}, received={len(batch)} deliveries")

            if not batch:
                break

            for item in batch:
                objs.append(DeliveryObj(
                    delivery_id=item['delivery_id'],
                    order_id=item['order_id'],
                    order_ts=item['order_ts'],
                    courier_id=item['courier_id'],
                    address=item['address'],
                    delivery_ts=item['delivery_ts'],
                    rate=item['rate'],
                    delivery_sum=item['sum'],
                    tip_sum=item['tip_sum'],
                ))

            offset += self.BATCH_SIZE

        return objs


class DeliveriesDestRepository:
    """Сохраняет доставки в stg.deliverysystem_deliveries."""

    def insert_delivery(self, conn: Any, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.deliverysystem_deliveries (object_id, object_value)
                VALUES (%(object_id)s, %(object_value)s)
                ON CONFLICT (object_id) DO UPDATE
                SET object_value = EXCLUDED.object_value;
                """,
                {
                    "object_id": delivery.delivery_id,
                    "object_value": json.dumps(delivery.dict()),
                },
            )


class DeliveriesLoader:
    """Оркестрирует инкрементальную загрузку доставок: API → STG."""

    WF_KEY = "deliveries_http_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    NUM_LOADED_KEY = "num_loaded"
    _LOG_THRESHOLD = 10

    def __init__(self, pg_conn: Any) -> None:
        self.pg_conn = pg_conn
        self.origin = DeliveriesOriginRepository()
        self.stg = DeliveriesDestRepository()

    def load_deliveries(self) -> None:
        with self.pg_conn as conn:
            # Читаем последнюю дату загрузки из wf_settings.
            # Если checkpoint нет — берём 7 дней назад (по условию задания).
            wf_setting = self._get_wf_setting(conn)
            default_start = (dt.datetime.now() - dt.timedelta(days=LOAD_DAYS_BACK)).strftime('%Y-%m-%d %H:%M:%S')
            last_loaded_ts = wf_setting.get(self.LAST_LOADED_TS_KEY, default_start)
            log.info(f"Deliveries: loading from checkpoint={last_loaded_ts}")

            # Загружаем из API (только новые записи с last_loaded_ts)
            load_queue = self.origin.list_deliveries(from_ts=last_loaded_ts)
            log.info(f"Deliveries: found {len(load_queue)} to load.")
            if not load_queue:
                log.info("Deliveries: nothing new to load, quitting.")
                return

            # Сохраняем в STG
            for i, delivery in enumerate(load_queue, 1):
                self.stg.insert_delivery(conn, delivery)
                if i % self._LOG_THRESHOLD == 0:
                    log.info(f"Deliveries: processed {i}/{len(load_queue)}")

            # Сохраняем прогресс: берём максимальный order_ts из загруженных
            max_ts = max(d.order_ts for d in load_queue)
            # Приводим к формату без микросекунд
            try:
                max_ts_dt = dt.datetime.strptime(max_ts, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                max_ts_dt = dt.datetime.strptime(max_ts, '%Y-%m-%d %H:%M:%S')

            wf_setting[self.LAST_LOADED_TS_KEY] = max_ts_dt.strftime('%Y-%m-%d %H:%M:%S')
            wf_setting[self.NUM_LOADED_KEY] = len(load_queue)
            self._save_wf_setting(conn, wf_setting)
            log.info(f"Deliveries: done, last_ts={wf_setting[self.LAST_LOADED_TS_KEY]}, total={len(load_queue)}")

    # --- helpers для workflow settings ---

    def _get_wf_setting(self, conn: Any) -> dict:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT workflow_settings FROM stg.srv_wf_settings WHERE workflow_key = %s",
                (self.WF_KEY,)
            )
            row = cur.fetchone()
        if row:
            value = row[0]
            return value if isinstance(value, dict) else json.loads(value)
        return {}

    def _save_wf_setting(self, conn: Any, settings: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%(key)s, %(value)s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {"key": self.WF_KEY, "value": json.dumps(settings)},
            )
