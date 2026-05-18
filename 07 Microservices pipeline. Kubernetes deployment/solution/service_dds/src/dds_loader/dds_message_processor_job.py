from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger
        self._source = 'stg-service'

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            # 1. Читаем сообщение из Kafka
            msg = self._consumer.consume()
            if msg is None:
                break

            payload = msg.get('payload', {})

            # 2. Обрабатываем только закрытые заказы
            if payload.get('status') != 'CLOSED':
                continue

            load_dt = datetime.utcnow()
            src = self._source

            # 3. Хабы — вставляем сущности, получаем их UUID
            user = payload['user']
            h_user_pk = self._dds_repository.h_user_insert(user['id'], load_dt, src)

            restaurant = payload['restaurant']
            h_restaurant_pk = self._dds_repository.h_restaurant_insert(restaurant['id'], load_dt, src)

            order_dt = datetime.strptime(payload['date'], '%Y-%m-%d %H:%M:%S')
            h_order_pk = self._dds_repository.h_order_insert(payload['id'], order_dt, load_dt, src)

            # 4. Саттелиты для заказа, пользователя, ресторана
            self._dds_repository.s_user_names_insert(h_user_pk, user['name'], user['name'], load_dt, src)
            self._dds_repository.s_restaurant_names_insert(h_restaurant_pk, restaurant['name'], load_dt, src)
            self._dds_repository.s_order_cost_insert(h_order_pk, payload['cost'], payload['payment'], load_dt, src)
            self._dds_repository.s_order_status_insert(h_order_pk, payload['status'], load_dt, src)

            # 5. Линк заказ-пользователь
            self._dds_repository.l_order_user_insert(h_order_pk, h_user_pk, load_dt, src)

            # 6. Продукты: хабы, саттелиты, линки
            products_out = []
            for product in payload['products']:
                h_product_pk = self._dds_repository.h_product_insert(product['id'], load_dt, src)
                h_category_pk = self._dds_repository.h_category_insert(product['category'], load_dt, src)

                self._dds_repository.s_product_names_insert(h_product_pk, product['name'], load_dt, src)
                self._dds_repository.l_order_product_insert(h_order_pk, h_product_pk, load_dt, src)
                self._dds_repository.l_product_restaurant_insert(h_product_pk, h_restaurant_pk, load_dt, src)
                self._dds_repository.l_product_category_insert(h_product_pk, h_category_pk, load_dt, src)

                products_out.append({
                    'id': str(h_product_pk),
                    'name': product['name'],
                    'category': {
                        'id': str(h_category_pk),
                        'name': product['category']
                    }
                })

            # 7. Формируем и отправляем выходное сообщение для CDM
            output = {
                'object_id': msg['object_id'],
                'object_type': msg['object_type'],
                'payload': {
                    'user': {'id': str(h_user_pk)},
                    'products': products_out
                }
            }
            self._producer.produce(output)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
