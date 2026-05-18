# Стриминг уведомлений для подписчиков ресторанов. Spark + Kafka.

## Описание задачи

Рестораны запускают рекламные акции и хотят уведомлять о них своих подписчиков в реальном времени. Нужно построить Spark Streaming сервис, который читает акции из Kafka, находит подписчиков каждого ресторана в PostgreSQL, фильтрует активные кампании и отправляет уведомления обратно в Kafka и в PostgreSQL для хранения истории фидбэков.

## Что было сделано

- Реализован Spark Structured Streaming сервис: чтение JSON-сообщений из Kafka-топика, парсинг по схеме через `from_json`
- JOIN потока акций с таблицей подписчиков из PostgreSQL (JDBC) внутри `foreachBatch` — каждый батч читает актуальные подписки, изменения подхватываются без рестарта job
- Фильтрация активных кампаний по текущему времени (`unix_timestamp()` между `datetime_start` и `datetime_end`)
- Идемпотентная запись в PostgreSQL через `INSERT ... ON CONFLICT DO NOTHING` — защита от дублей при повторной обработке батча после рестарта
- Отправка результата в выходной Kafka-топик через `to_json(struct("*"))`
- Checkpoint-based восстановление: при рестарте обработка продолжается с последнего сохранённого офсета без потери сообщений

## Стек и инструменты

`Apache Spark` `Spark Structured Streaming` `Apache Kafka` `PostgreSQL` `Python` `JDBC`

## Структура папки

- `src/scripts/streaming_project.py` — основной код сервиса
- `.env.example` — шаблон переменных окружения (Kafka, PostgreSQL)
- `REVIEW_RESPONSES.md` — описание доработок по результатам ревью

## Ход реализации

### Сервис стриминговых уведомлений для подписчиков ресторанов

Spark Streaming сервис, который читает рекламные акции ресторанов из Kafka, находит подписчиков этих ресторанов в PostgreSQL, формирует уведомления и отправляет их обратно в Kafka и сохраняет в PostgreSQL для аналитики фидбэков.

```
Kafka (topic_in) → Spark Streaming → JOIN с PostgreSQL (подписчики)
                                    → фильтрация активных кампаний
                                    → Kafka (topic_out)
                                    → PostgreSQL (subscribers_feedback)
```

---

### Настройка окружения

Скопируй `.env.example` в `.env` и заполни значения:

```bash
cp .env.example .env
```

Переменные окружения:

| Переменная | Описание |
|---|---|
| `POSTGRES_HOST` | Хост PostgreSQL для записи фидбэков |
| `POSTGRES_PORT` | Порт (по умолчанию 5432) |
| `POSTGRES_USER` | Пользователь |
| `POSTGRES_PASSWORD` | Пароль |
| `SOURCE_POSTGRES_HOST` | Хост PostgreSQL с таблицей подписчиков |
| `SOURCE_POSTGRES_PORT` | Порт (по умолчанию 6432) |
| `SOURCE_POSTGRES_USER` | Пользователь |
| `SOURCE_POSTGRES_PASSWORD` | Пароль |
| `KAFKA_BOOTSTRAP_SERVERS` | Адрес Kafka брокера |
| `KAFKA_USER` | Пользователь Kafka |
| `KAFKA_PASSWORD` | Пароль Kafka |
| `KAFKA_TOPIC_IN` | Входной топик с акциями ресторанов |
| `KAFKA_TOPIC_OUT` | Выходной топик для push-уведомлений |

---

### Подготовка БД

Перед первым запуском создать таблицу и уникальный индекс в PostgreSQL:

```sql
CREATE TABLE public.subscribers_feedback (
    id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int8 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);

-- индекс для идемпотентной записи (защита от дублей при рестарте job)
CREATE UNIQUE INDEX IF NOT EXISTS uix_feedback_notification
    ON public.subscribers_feedback (restaurant_id, adv_campaign_id, client_id);
```

---

### Запуск

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.0 \
  src/scripts/streaming_project.py
```

Переменные из `.env` должны быть экспортированы в окружение перед запуском, либо переданы через `docker-compose`.

> **Checkpoint:** сервис сохраняет состояние в `/tmp/checkpoints/restaurant_subscribe`.
> При рестарте обработка продолжается с последнего сохранённого офсета — сообщения не теряются.
> В продакшне `/tmp` следует заменить на постоянное хранилище (HDFS, S3), так как содержимое `/tmp` очищается при перезагрузке контейнера.

---

### Проверка Kafka перед запуском

**Консьюмер** — слушаем входной топик:

```bash
kafkacat -b <KAFKA_BOOTSTRAP_SERVERS> \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username="<KAFKA_USER>" \
  -X sasl.password="<KAFKA_PASSWORD>" \
  -X ssl.ca.location="/root/CA/CA.pem" \
  -t <KAFKA_TOPIC_IN> -K: -C
```

**Продюсер** — отправляем тестовое сообщение:

```bash
kafkacat -b <KAFKA_BOOTSTRAP_SERVERS> \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username="<KAFKA_USER>" \
  -X sasl.password="<KAFKA_PASSWORD>" \
  -X ssl.ca.location="/root/CA/CA.pem" \
  -t <KAFKA_TOPIC_IN> -K: -P
```

Пример тестового сообщения:

```
key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant.ru","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}
```

---

### Пошаговое описание пайплайна

#### Шаг 1. Чтение акций из Kafka

`restaurant_read()` читает поток из входного топика и парсит JSON по схеме.

Ключевые моменты:
- `readStream` вместо `read` — потоковое чтение
- `value` в Kafka хранится как `binary`, нужно привести к строке через `.cast("string")`
- `from_json` парсит JSON по заданной схеме

```python
def restaurant_read(spark: SparkSession) -> DataFrame:
    incomming_message_schema = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", LongType()),
        StructField("adv_campaign_datetime_end", LongType()),
        StructField("datetime_created", LongType())
    ])

    return spark.readStream \
        .format('kafka') \
        .options(**kafka_security_options) \
        .option('subscribe', KAFKA_TOPIC_IN) \
        .option('startingOffsets', 'latest') \
        .load() \
        .withColumn("value", F.col("value").cast("string")) \
        .withColumn("parsed", F.from_json(F.col("value"), incomming_message_schema)) \
        .select("parsed.*")
```

---

#### Шаг 2. Чтение подписчиков из PostgreSQL

`read_subscribers_restaurants()` читает таблицу подписчиков через JDBC. Вызывается внутри `foreach_batch_function` на каждый батч — так в обработку всегда попадают актуальные подписки и отписки.

```python
def read_subscribers_restaurants(spark: SparkSession, postgresql_settings: dict) -> DataFrame:
    return spark.read \
        .format("jdbc") \
        .options(**postgresql_settings) \
        .load()
```

---

#### Шаг 3. JOIN и фильтрация активных кампаний

`join()` джойнит поток акций с подписчиками по `restaurant_id` и оставляет только те кампании, у которых текущее время попадает между `adv_campaign_datetime_start` и `adv_campaign_datetime_end`.

Ключевые моменты:
- `F.unix_timestamp()` — текущее время в секундах, вычисляется Spark-ом на каждый батч в UTC
- стриминговый датафрейм всегда слева в join

```python
def join(df_stream: DataFrame, df_static: DataFrame) -> DataFrame:
    df_static = df_static.drop("id")
    return df_stream \
        .join(df_static, on="restaurant_id", how="inner") \
        .filter(
            (F.unix_timestamp() >= F.col("adv_campaign_datetime_start")) &
            (F.unix_timestamp() <= F.col("adv_campaign_datetime_end"))
        ) \
        .withColumn("trigger_datetime_created", F.unix_timestamp().cast(LongType()))
```

---

#### Шаг 4. Запись в PostgreSQL

`write_to_postgres()` сохраняет батч через `INSERT ... ON CONFLICT DO NOTHING` — при повторной обработке батча после рестарта дубли в таблицу не попадут.

```python
def write_to_postgres(df: DataFrame, postgresql_settings: dict) -> None:
    df.withColumn("feedback", F.lit(None).cast(StringType())) \
        .write \
        .format("jdbc") \
        .mode("append") \
        .options(**postgresql_settings) \
        .option(
            "insertStatement",
            "INSERT INTO public.subscribers_feedback "
            "(restaurant_id, adv_campaign_id, adv_campaign_content, "
            "adv_campaign_owner, adv_campaign_owner_contact, "
            "adv_campaign_datetime_start, adv_campaign_datetime_end, "
            "datetime_created, client_id, trigger_datetime_created, feedback) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (restaurant_id, adv_campaign_id, client_id) DO NOTHING"
        ) \
        .save()
```

---

#### Шаг 5. Отправка в Kafka

Сериализуем все поля в JSON и отправляем в выходной топик.

Ключевые моменты:
- Kafka ожидает колонку `value` с данными
- `F.to_json(F.struct("*"))` сериализует все колонки датафрейма в JSON строку
- внутри `foreachBatch` используется `df.write` (не `writeStream`)

```python
df \
    .withColumn("value", F.to_json(F.struct("*"))) \
    .select("value") \
    .write \
    .format("kafka") \
    .options(**kafka_security_options) \
    .option("topic", KAFKA_TOPIC_OUT) \
    .save()
```

---

#### Шаг 6. foreachBatch

`foreach_batch_function()` объединяет все шаги для каждого микробатча. `persist()/unpersist()` обёрнуты в `try/finally` — кэш освобождается при любом исходе, в том числе при исключении.

```python
def foreach_batch_function(df: DataFrame, epoch_id: int) -> None:
    if df.rdd.isEmpty():
        return

    spark = df.sparkSession
    df_subscribers = read_subscribers_restaurants(spark, postgresql_settings_source_DB)
    df = join(df, df_subscribers)

    df.persist()
    try:
        write_to_postgres(df, postgresql_settings_in_Docker)
        df \
            .withColumn("value", F.to_json(F.struct("*"))) \
            .select("value") \
            .write \
            .format("kafka") \
            .options(**kafka_security_options) \
            .option("topic", KAFKA_TOPIC_OUT) \
            .save()
    except Exception as e:
        raise RuntimeError(f"foreach_batch_function failed on epoch {epoch_id}: {e}") from e
    finally:
        df.unpersist()
```

---

### Схема входного сообщения Kafka

```json
{
  "restaurant_id": "uuid",
  "adv_campaign_id": "uuid",
  "adv_campaign_content": "текст акции",
  "adv_campaign_owner": "имя владельца",
  "adv_campaign_owner_contact": "контакт",
  "adv_campaign_datetime_start": 1659203516,
  "adv_campaign_datetime_end": 2659207116,
  "datetime_created": 1659131516
}
```

---

### Ключевые концепции

**Kafka** — брокер сообщений. Принимает данные от продюсера, хранит и отдаёт консьюмеру. Данные хранятся в топиках, топики делятся на партиции.

**Spark Structured Streaming** — обрабатывает данные в режиме реального времени. Основное отличие от батчевой обработки: `readStream` вместо `read`, `writeStream` вместо `write`.

**foreachBatch** — позволяет применить произвольную функцию к каждому микробатчу. Используется когда нужно записать данные в несколько стоков или применить логику, недоступную в стриминговом API.

**persist/unpersist** — кэширование датафрейма в памяти чтобы избежать повторных вычислений при использовании датафрейма несколько раз в одном батче.

**JDBC** — стандартный интерфейс для подключения к реляционным БД из Java/Spark. Требует драйвер конкретной БД (для PostgreSQL — `org.postgresql.Driver`).

**Checkpoint** — механизм сохранения состояния стрима (офсеты, прогресс). Позволяет продолжить обработку с последней позиции после рестарта job без потери данных.
