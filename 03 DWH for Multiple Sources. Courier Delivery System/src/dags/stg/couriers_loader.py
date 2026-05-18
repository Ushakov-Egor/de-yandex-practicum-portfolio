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


class CourierObj(BaseModel):
    courier_id: str
    name: str


class CouriersOriginRepository:
    """Загружает курьеров из HTTP API с пагинацией."""

    BATCH_SIZE = 50
    ENDPOINT = '/couriers'

    def list_couriers(self) -> List[CourierObj]:
        url = f"{API_BASE_URL}{self.ENDPOINT}"
        objs = []
        offset = 0

        while True:
            params = {
                'sort_field': 'id',
                'sort_direction': 'asc',
                'limit': self.BATCH_SIZE,
                'offset': offset,
            }
            rs = requests.get(url, headers=API_HEADERS, params=params)

            if rs.status_code != 200:
                raise Exception(f"API error: status_code={rs.status_code}, body={rs.text}")

            batch = json.loads(rs.content)
            log.info(f"offset={offset}, received={len(batch)} couriers")

            if not batch:
                break

            for item in batch:
                objs.append(CourierObj(
                    courier_id=item['_id'],
                    name=item['name'],
                ))

            offset += self.BATCH_SIZE

        return objs


class CouriersDestRepository:
    """Сохраняет курьеров в stg.deliverysystem_couriers."""

    def insert_courier(self, conn: Any, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.deliverysystem_couriers (object_id, object_value)
                VALUES (%(object_id)s, %(object_value)s)
                ON CONFLICT (object_id) DO UPDATE
                SET object_value = EXCLUDED.object_value;
                """,
                {
                    "object_id": courier.courier_id,
                    "object_value": json.dumps({"_id": courier.courier_id, "name": courier.name}),
                },
            )


class CouriersLoader:
    """Оркестрирует полную загрузку курьеров: API → STG."""

    WF_KEY = "couriers_http_to_stg_workflow"
    NUM_LOADED_KEY = "num_loaded"
    LAST_LOAD_TIME_KEY = "last_load_time"
    _LOG_THRESHOLD = 10

    def __init__(self, pg_conn: Any) -> None:
        self.pg_conn = pg_conn
        self.origin = CouriersOriginRepository()
        self.stg = CouriersDestRepository()

    def load_couriers(self) -> None:
        with self.pg_conn as conn:
            # Читаем состояние прогресса из БД
            wf_setting = self._get_wf_setting(conn)
            log.info(f"Couriers: previously loaded {wf_setting.get(self.NUM_LOADED_KEY, 0)}")

            # Загружаем из API
            load_queue = self.origin.list_couriers()
            log.info(f"Couriers: found {len(load_queue)} to load.")
            if not load_queue:
                log.info("Couriers: nothing to load, quitting.")
                return

            # Сохраняем в STG
            for i, courier in enumerate(load_queue, 1):
                self.stg.insert_courier(conn, courier)
                if i % self._LOG_THRESHOLD == 0:
                    log.info(f"Couriers: processed {i}/{len(load_queue)}")

            # Сохраняем прогресс
            wf_setting[self.NUM_LOADED_KEY] = len(load_queue)
            wf_setting[self.LAST_LOAD_TIME_KEY] = str(dt.datetime.now())
            self._save_wf_setting(conn, wf_setting)
            log.info(f"Couriers: done, total loaded={len(load_queue)}")

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
