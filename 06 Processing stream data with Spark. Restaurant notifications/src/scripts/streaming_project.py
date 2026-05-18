import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, to_json, col, lit, struct
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])

# настройки подключения к PostgreSQL для записи фидбэков.
# Хост и порт берутся из переменных окружения, чтобы не завязываться на конкретный
# способ запуска (localhost работает только если Spark и PostgreSQL в одной сетевой namespace).
# В docker-compose задать: POSTGRES_HOST=<имя сервиса>, POSTGRES_PORT=5432
postgresql_settings_in_Docker = {
    'url': f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'localhost')}:{os.environ.get('POSTGRES_PORT', '5432')}/de",
    'driver': 'org.postgresql.Driver',
    'user': os.environ.get('POSTGRES_USER', 'jovyan'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'jovyan'),
    'dbtable': 'public.subscribers_feedback'
}

# настройки подключения к удалённому PostgreSQL в Яндекс Облаке (для чтения подписчиков)
postgresql_settings_source_DB = {
    'url': f"jdbc:postgresql://{os.environ.get('SOURCE_POSTGRES_HOST')}:{os.environ.get('SOURCE_POSTGRES_PORT', '6432')}/de",
    'driver': 'org.postgresql.Driver',
    'user': os.environ.get('SOURCE_POSTGRES_USER'),
    'password': os.environ.get('SOURCE_POSTGRES_PASSWORD'),
    'dbtable': 'public.subscribers_restaurants'
}

# настройки безопасности для подключения к Kafka
kafka_security_options = {
    'kafka.bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.jaas.config': (
        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{os.environ.get("KAFKA_USER")}" '
        f'password="{os.environ.get("KAFKA_PASSWORD")}";'
    ),
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
}

KAFKA_TOPIC_IN = os.environ.get('KAFKA_TOPIC_IN', 'topic_in')
KAFKA_TOPIC_OUT = os.environ.get('KAFKA_TOPIC_OUT', 'topic_out')


def spark_init(spark_jars_packages: str) -> SparkSession:
    """Создаём SparkSession с необходимыми библиотеками для Kafka и PostgreSQL"""
    return SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()


def restaurant_read(spark: SparkSession) -> DataFrame:
    """Читаем поток сообщений об акциях из Kafka и парсим JSON"""

    # схема входного сообщения
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
        .select(
            F.col("parsed.restaurant_id").alias("restaurant_id"),
            F.col("parsed.adv_campaign_id").alias("adv_campaign_id"),
            F.col("parsed.adv_campaign_content").alias("adv_campaign_content"),
            F.col("parsed.adv_campaign_owner").alias("adv_campaign_owner"),
            F.col("parsed.adv_campaign_owner_contact").alias("adv_campaign_owner_contact"),
            F.col("parsed.adv_campaign_datetime_start").alias("adv_campaign_datetime_start"),
            F.col("parsed.adv_campaign_datetime_end").alias("adv_campaign_datetime_end"),
            F.col("parsed.datetime_created").alias("datetime_created")
        )


def read_subscribers_restaurants(spark: SparkSession, postgresql_settings: dict) -> DataFrame:
    """Читаем статичную таблицу подписчиков ресторанов из PostgreSQL"""
    return spark.read \
        .format("jdbc") \
        .options(**postgresql_settings) \
        .load()


def join(df_stream: DataFrame, df_static: DataFrame) -> DataFrame:
    """Джойним поток акций с подписчиками по restaurant_id, фильтруем только активные кампании
    и добавляем время создания триггера"""
    df_static = df_static.drop("id")
    return df_stream \
        .join(df_static, on="restaurant_id", how="inner") \
        .filter(
            (F.unix_timestamp() >= F.col("adv_campaign_datetime_start")) &
            (F.unix_timestamp() <= F.col("adv_campaign_datetime_end"))
        ) \
        .withColumn("trigger_datetime_created", F.unix_timestamp().cast(LongType()))


def write_to_postgres(df: DataFrame, postgresql_settings: dict) -> None:
    """Записываем батч в PostgreSQL idempotently через INSERT ... ON CONFLICT DO NOTHING.

    Уникальность уведомления определяется по (restaurant_id, adv_campaign_id, client_id).
    При повторной обработке батча (рестарт job) дубли в таблицу не попадут.
    Требует уникального индекса на таблице:
        CREATE UNIQUE INDEX IF NOT EXISTS uix_feedback_notification
        ON public.subscribers_feedback (restaurant_id, adv_campaign_id, client_id);
    """
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


def foreach_batch_function(df: DataFrame, epoch_id: int) -> None:
    """Обрабатываем каждый микробатч — записываем в PostgreSQL и Kafka"""

    # пропускаем пустые батчи, чтобы не делать лишних коннектов к PostgreSQL и Kafka
    if df.rdd.isEmpty():
        return

    # читаем актуальных подписчиков на каждый батч, чтобы учитывать новые подписки и отписки
    spark = df.sparkSession
    df_subscribers = read_subscribers_restaurants(spark, postgresql_settings_source_DB)
    df = join(df, df_subscribers)

    # сохраняем df в памяти чтобы не пересчитывать дважды
    df.persist()
    try:
        # записываем в PostgreSQL для аналитики фидбэков
        write_to_postgres(df, postgresql_settings_in_Docker)

        # сериализуем все поля в JSON и отправляем в Kafka для push-уведомлений
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
        # освобождаем память при любом исходе
        df.unpersist()


if __name__ == "__main__":
    # инициализация
    spark = spark_init(spark_jars_packages)

    # читаем поток акций из Kafka
    df_restaurant_read_stream = restaurant_read(spark)

    # запускаем стриминг (джойн с подписчиками происходит внутри foreach_batch_function)
    query = (df_restaurant_read_stream
             .writeStream
             .outputMode("append")
             .foreachBatch(foreach_batch_function)
             .option("checkpointLocation", "/tmp/checkpoints/restaurant_subscribe")
             .start())

    query.awaitTermination()