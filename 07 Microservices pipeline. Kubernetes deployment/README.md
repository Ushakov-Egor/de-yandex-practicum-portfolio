# Микросервисный пайплайн обработки заказов. Kubernetes.

## Описание задачи

Сервис заказов генерирует события в Kafka, которые нужно обработать в режиме реального времени и построить аналитические счётчики по пользователям. Решение реализовано как три независимых микросервиса, последовательно трансформирующих данные через слои STG → DDS → CDM, задеплоенных в Kubernetes через Helm.

## Что было сделано

- STG-сервис: читает сырые заказы из Kafka, сохраняет в PostgreSQL и пробрасывает обогащённые сообщения в следующий топик
- DDS-сервис: трансформирует данные в Data Vault (5 хабов, 4 линка, 5 сателлитов), генерирует детерминированные UUID-ключи через `uuid.uuid5` для идемпотентности, обрабатывает только заказы со статусом `CLOSED`
- CDM-сервис: строит аналитические счётчики `user_product_counters` и `user_category_counters` через `INSERT ... ON CONFLICT DO UPDATE SET order_cnt = order_cnt + 1`
- Каждый сервис упакован в Docker-образ, задеплоен в Kubernetes (Yandex Cloud) через Helm-чарт с ConfigMap для передачи параметров Kafka и PostgreSQL
- Все INSERT идемпотентны через `ON CONFLICT DO NOTHING` / `DO UPDATE` — повторная обработка сообщения не создаёт дублей

## Стек и инструменты

`Python` `Apache Kafka` `PostgreSQL` `Kubernetes` `Helm` `Docker` `Flask` `APScheduler` `Data Vault`

## Структура папки

- `solution/` — исходный код всех трёх сервисов
  - `service_stg/` — STG-сервис (чтение из Kafka → PostgreSQL)
  - `service_dds/` — DDS-сервис (трансформация в Data Vault)
  - `service_cdm/` — CDM-сервис (аналитические счётчики)
  - Каждый сервис содержит: `app/` (код), `Dockerfile`, `helm/` (чарт для Kubernetes)

## Ход выполнения

## Проект 9-го спринта

### Container Registry
- **STG Service:** `cr.yandex/crpj3iasc4936bcftfp0/stg_service`
- **DDS Service:** `cr.yandex/crpj3iasc4936bcftfp0/dds_service`
- **CDM Service:** `cr.yandex/crpj3iasc4936bcftfp0/cdm_service`

---

### Описание реализации

#### Архитектура системы

Система состоит из трёх микросервисов, реализующих потоковую обработку данных о заказах:

```
Kafka (order-service_orders)
        ↓
   STG-Service  →  stg.order_events (PostgreSQL)
        ↓ Kafka (stg-service-orders)
   DDS-Service  →  dds.h_*, dds.l_*, dds.s_* (PostgreSQL)
        ↓ Kafka (dds-service-orders)
   CDM-Service  →  cdm.user_product_counters, cdm.user_category_counters (PostgreSQL)
```

Каждый сервис реализован на Python 3.10 с использованием Flask + APScheduler (интервал запуска — 25 секунд). Все сервисы задеплоены в Kubernetes (Yandex Cloud) через Helm.

---

### Шаг 0. Проверка готовности шаблонов

Репозиторий содержит два шаблона сервисов (`service_dds` и `service_cdm`) с готовой инфраструктурной частью:
- подключение к Kafka (confluent-kafka, SASL_SSL, SCRAM-SHA-512)
- подключение к PostgreSQL (psycopg3, SSL)
- Flask-приложение с APScheduler
- Helm-чарт для деплоя в Kubernetes

Шаблоны скопированы из репозитория курса и доработаны.

---

### Шаг 1. Создание DDS-сервиса: планирование

**DoD сервиса:**
- Читает обогащённые сообщения из топика `stg-service-orders`
- Обрабатывает только заказы со статусом `CLOSED`
- Заполняет слой DDS по методологии Data Vault
- Отправляет агрегированное сообщение в топик `dds-service-orders`

**Входная точка:** топик Kafka `stg-service-orders`. Сообщение содержит:
```json
{
  "object_id": 12345,
  "object_type": "order",
  "payload": {
    "id": 12345,
    "date": "2026-04-11 17:49:08",
    "cost": 2700,
    "payment": 2700,
    "status": "CLOSED",
    "restaurant": {"id": "...", "name": "..."},
    "user": {"id": "...", "name": "..."},
    "products": [
      {"id": "...", "name": "...", "price": 180, "quantity": 3, "category": "..."}
    ]
  }
}
```

**Путь обработки:**
1. Прочитать сообщение из Kafka
2. Отфильтровать: обрабатывать только `status == CLOSED`
3. Сформировать UUID-ключи через `uuid.uuid5(NAMESPACE_X500, business_key)`
4. Вставить хабы, потом сателлиты, потом линки (порядок важен для FK)
5. Сформировать выходное сообщение для CDM и отправить в `dds-service-orders`

**Что нужно создать:**
- Топик Kafka `dds-service-orders` (создан вручную в Yandex Cloud Console)
- DDL-таблицы DDS (уже были созданы ранее)
- Код `dds_repository.py` и `dds_message_processor_job.py`
- Dockerfile, Helm-чарт, Docker-образ

---

### Шаг 2. Создание DDS-сервиса: код

#### Структура Data Vault

**Хабы (5 штук):** хранят бизнес-ключи сущностей
- `dds.h_user` — пользователи
- `dds.h_restaurant` — рестораны
- `dds.h_product` — продукты
- `dds.h_category` — категории блюд
- `dds.h_order` — заказы

**Линки (4 штуки):** хранят связи между сущностями
- `dds.l_order_product` — заказ ↔ продукт
- `dds.l_order_user` — заказ ↔ пользователь
- `dds.l_product_restaurant` — продукт ↔ ресторан
- `dds.l_product_category` — продукт ↔ категория

**Сателлиты (5 штук):** хранят атрибуты сущностей
- `dds.s_user_names` — имя пользователя
- `dds.s_restaurant_names` — название ресторана
- `dds.s_product_names` — название продукта
- `dds.s_order_cost` — стоимость и оплата заказа
- `dds.s_order_status` — статус заказа

#### Генерация UUID-ключей

Все первичные ключи генерируются детерминированно через `uuid.uuid5`, что обеспечивает идемпотентность:

```python
h_user_pk = uuid.uuid5(uuid.NAMESPACE_X500, user_id)
hk_order_user = uuid.uuid5(uuid.NAMESPACE_X500, str(h_order_pk) + str(h_user_pk))
```

#### Идемпотентность

Все INSERT-запросы используют `ON CONFLICT (pk) DO NOTHING` — повторная обработка одного сообщения не создаёт дублей.

#### Выходное сообщение для CDM

```json
{
  "object_id": 12345,
  "object_type": "order",
  "payload": {
    "user": {"id": "<uuid>"},
    "products": [
      {"id": "<uuid>", "name": "...", "category": {"id": "<uuid>", "name": "..."}}
    ]
  }
}
```

В выходном сообщении используются UUID-ключи из DDS, а не исходные строковые идентификаторы.

---

### Шаг 3. Создание DDS-сервиса: деплой в Kubernetes

**Dockerfile** написан на основе шаблона STG-сервиса. Подключение к Kafka и PostgreSQL через переменные окружения из Kubernetes ConfigMap.

**Сборка и публикация образа:**
```bash
docker build -t cr.yandex/crpj3iasc4936bcftfp0/dds_service:v2026-04-18-r1 .
docker push cr.yandex/crpj3iasc4936bcftfp0/dds_service:v2026-04-18-r1
```

**Helm-чарт** содержит:
- `Chart.yaml` — метаданные чарта
- `values.yaml` — конфигурация: образ, порт, параметры Kafka и PostgreSQL
- `templates/deployment.yaml` — Deployment с `envFrom: configMapRef`
- `templates/configmap.yaml` — ConfigMap с переменными окружения

**Деплой:**
```bash
helm upgrade --install dds-service app -n <namespace>
```

**Конфигурация сервиса (`values.yaml`):**
```yaml
KAFKA_SOURCE_TOPIC: "stg-service-orders"
KAFKA_DESTINATION_TOPIC: "dds-service-orders"
KAFKA_CONSUMER_GROUP: "dds-service-consumer"
```

**Проверка:**
```bash
kubectl get pods -n <namespace>
# dds-service-xxx   1/1   Running
```

**Результат в PostgreSQL:** таблицы DDS заполнены данными (хабы, линки, сателлиты).

---

### Шаг 4. Создание CDM-сервиса: планирование

**DoD сервиса:**
- Читает сообщения из топика `dds-service-orders`
- Обновляет счётчики заказов пользователей по продуктам и категориям
- Конец пайплайна — выходной топик Kafka не нужен

**Входная точка:** топик `dds-service-orders` с UUID-ключами из DDS.

**Путь обработки:**
1. Прочитать сообщение из Kafka
2. Для каждого продукта в заказе:
   - Upsert в `cdm.user_product_counters` (инкремент счётчика)
   - Upsert в `cdm.user_category_counters` (инкремент счётчика)

**Витрины CDM:**
- `cdm.user_product_counters` — сколько раз пользователь заказывал каждый продукт
- `cdm.user_category_counters` — сколько раз пользователь заказывал из каждой категории

---

### Шаг 5. Создание CDM-сервиса: код

#### Логика обработки

```python
for product in payload['products']:
    product_id = UUID(product['id'])
    category_id = UUID(product['category']['id'])

    cdm_repository.user_product_counters_upsert(user_id, product_id, product_name)
    cdm_repository.user_category_counters_upsert(user_id, category_id, category_name)
```

#### Идемпотентность

Витрины используют `ON CONFLICT DO UPDATE SET order_cnt = order_cnt + 1`. При повторной обработке одного и того же сообщения счётчик инкрементируется повторно, поэтому обеспечивается идемпотентность на уровне consumer group offset — Kafka гарантирует обработку каждого сообщения ровно один раз в рамках группы с `enable.auto.commit: False` и ручным коммитом.

#### SQL для user_product_counters

```sql
INSERT INTO cdm.user_product_counters(user_id, product_id, product_name, order_cnt)
VALUES (%(user_id)s, %(product_id)s, %(product_name)s, 1)
ON CONFLICT (user_id, product_id) DO UPDATE SET
    order_cnt = cdm.user_product_counters.order_cnt + 1,
    product_name = EXCLUDED.product_name;
```

#### SQL для user_category_counters

```sql
INSERT INTO cdm.user_category_counters(user_id, category_id, category_name, order_cnt)
VALUES (%(user_id)s, %(category_id)s, %(category_name)s, 1)
ON CONFLICT (user_id, category_id) DO UPDATE SET
    order_cnt = cdm.user_category_counters.order_cnt + 1,
    category_name = EXCLUDED.category_name;
```

CDM-сервис не имеет Kafka Producer — это конечная точка пайплайна.

---

### Шаг 6. Создание CDM-сервиса: деплой в Kubernetes

**Сборка и публикация образа:**
```bash
docker build -t cr.yandex/crpj3iasc4936bcftfp0/cdm_service:v2026-04-18-r1 .
docker push cr.yandex/crpj3iasc4936bcftfp0/cdm_service:v2026-04-18-r1
```

**Деплой:**
```bash
helm upgrade --install cdm-service app -n <namespace>
```

**Конфигурация сервиса (`values.yaml`):**
```yaml
KAFKA_SOURCE_TOPIC: "dds-service-orders"
KAFKA_CONSUMER_GROUP: "cdm-service-consumer"
```

**Проверка:**
```bash
kubectl get pods -n <namespace>
# cdm-service-xxx   1/1   Running
```

**Результат в PostgreSQL:**
```sql
SELECT COUNT(*) FROM cdm.user_product_counters;   -- данные есть
SELECT COUNT(*) FROM cdm.user_category_counters;  -- данные есть
```

Витрины заполняются данными в режиме реального времени по мере поступления заказов.

---

### Сложности и ошибки, а также их решения

#### 1. Двойной порт в конфигурации Kafka
В `values.yaml` значение `KAFKA_HOST` содержало порт (`:9091`), при этом отдельно существовал параметр `KAFKA_PORT: 9091`. В результате адрес брокера формировался как `host:9091:9091`, что ломало подключение. Исправлено удалением порта из значения `KAFKA_HOST`.

#### 2. Отсутствие KAFKA_DESTINATION_TOPIC
В `values.yaml` STG-сервиса параметр выходного топика назывался `KAFKA_STG_SERVICE_ORDERS_TOPIC`, тогда как код читал переменную `KAFKA_DESTINATION_TOPIC`. Добавлен корректный параметр с нужным именем.

#### 3. Consumer group на конце топика
Все ранее использованные consumer group (`stg-service-consumer`, `-2`, `-3`) уже прочитали все 1403 сообщения и сохранили offset в конце топика. Создание новых групп не помогало — сообщения истекли по retention policy Kafka, и топик оказался физически пуст. Проблема решена регистрацией Kafka в сервисе-генераторе Практикума (`order-gen-service.sprint9.tgcloudenv.ru`), который начал поставлять новые сообщения.

#### 4. Превышение квоты подов в Kubernetes
Namespace ограничен тремя подами. При стратегии Rolling Update Kubernetes сначала создаёт новый под, и только потом удаляет старый — в результате кратковременно требовалось 4 пода, что превышало квоту. Деплой зависал с ошибкой `exceeded quota`. Решение: использование `helm uninstall` с последующим `helm upgrade --install` вместо простого `helm upgrade`.

#### 5. Несоответствие структуры данных из Kafka
Реальная структура сообщений от генератора отличалась от ожидаемой в коде:
- Список товаров в payload назывался `order_items`, а не `products`
- Поле `payload['id']` отсутствовало — идентификатор заказа брался из `msg['object_id']`
- Поле `payload['status']` отсутствовало — использовалось `payload['final_status']`

Код STG-сервиса исправлен в соответствии с реальной структурой данных.

#### 6. MongoDB-стиль ключей в Redis
Данные о ресторанах в Redis хранились в MongoDB-формате: поле идентификатора у элементов меню называлось `_id` вместо `id`. В коде применено обращение `item.get('id') or item.get('_id', '')` для поддержки обоих вариантов. Для идентификаторов ресторана и пользователя в выходном сообщении стали использоваться значения, полученные непосредственно из payload Kafka, а не из объекта Redis.

#### 7. Отсутствие автоинкремента и уникального ограничения в PostgreSQL
В таблице `stg.order_events` колонка `id` была объявлена без `GENERATED ALWAYS AS IDENTITY`, из-за чего INSERT падал с ошибкой `null value in column "id"`. Также отсутствовал UNIQUE constraint на `object_id`, необходимый для работы `ON CONFLICT (object_id)`. Оба ограничения добавлены через `ALTER TABLE`. Аналогичная проблема с автоинкрементом была в таблице `cdm.user_product_counters`.
