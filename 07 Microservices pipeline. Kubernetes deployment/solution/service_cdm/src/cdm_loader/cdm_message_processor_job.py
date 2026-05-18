from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            # 1. Читаем сообщение из Kafka
            msg = self._consumer.consume()
            if msg is None:
                break

            payload = msg.get('payload', {})
            user_id = UUID(payload['user']['id'])

            # 2. Для каждого продукта обновляем счётчики
            for product in payload['products']:
                product_id = UUID(product['id'])
                product_name = product['name']

                category_id = UUID(product['category']['id'])
                category_name = product['category']['name']

                # Счётчик по продуктам
                self._cdm_repository.user_product_counters_upsert(
                    user_id, product_id, product_name
                )

                # Счётчик по категориям
                self._cdm_repository.user_category_counters_upsert(
                    user_id, category_id, category_name
                )

        self._logger.info(f"{datetime.utcnow()}: FINISH")
