import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_friends_mart(spark, date: str, depth: int, events_base_path: str, output_base_path: str):
    """
    Строит витрину рекомендации друзей friends_mart.
    Рекомендуем пару если: подписаны на один канал, никогда не переписывались, расстояние <= 1 км.
    Поля: user_left, user_right, processed_dttm, zone_id, local_time
    """

    EARTH_RADIUS = 6371

    def haversine(lat1, lon1, lat2, lon2):
        """Формула Haversine — расстояние между двумя точками на сфере в км."""
        lat1_r = F.radians(F.col(lat1))
        lat2_r = F.radians(F.col(lat2))
        lon1_r = F.radians(F.col(lon1))
        lon2_r = F.radians(F.col(lon2))

        return 2 * EARTH_RADIUS * F.asin(
            F.sqrt(
                F.pow(F.sin((lat2_r - lat1_r) / 2), 2)
                + F.cos(lat1_r) * F.cos(lat2_r)
                * F.pow(F.sin((lon2_r - lon1_r) / 2), 2)
            )
        )

    # Читаем только нужный период — depth дней до date
    dates = [(datetime.strptime(date, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(depth)]

    events_with_city = spark.read.parquet(events_base_path) \
        .filter(F.col('date').isin(dates))

    # Шаг 1: все пользователи для каждого канала
    group_users = events_with_city \
        .filter(
            (F.col('event_type') == 'message') &
            F.col('event.message_group').isNotNull() &
            F.col('event.message_from').isNotNull()
        ) \
        .select(
            F.col('event.message_group').alias('group_id'),
            F.col('event.message_from').alias('user_id')
        ) \
        .distinct()

    # Шаг 2: уникальные пары пользователей внутри каждого канала
    # u1.user_id < u2.user_id исключает дубли (u1,u2) и (u2,u1) и пары (u,u)
    group_pairs = group_users.alias('u1') \
        .join(group_users.alias('u2'), on='group_id') \
        .filter(F.col('u1.user_id') < F.col('u2.user_id')) \
        .select(
            F.col('group_id'),
            F.col('u1.user_id').alias('user_left'),
            F.col('u2.user_id').alias('user_right')
        ) \
        .distinct()

    # Шаг 3: актуальные координаты каждого пользователя (последнее сообщение)
    w_last = Window.partitionBy('event.message_from').orderBy(F.col('date').desc())

    user_coordinates = events_with_city \
        .filter(
            (F.col('event_type') == 'message') &
            F.col('event.message_from').isNotNull() &
            F.col('lat').isNotNull() &
            F.col('lon').isNotNull()
        ) \
        .withColumn('rnk', F.row_number().over(w_last)) \
        .filter(F.col('rnk') == 1) \
        .select(
            F.col('event.message_from').alias('user_id'),
            F.col('lat'),
            F.col('lon'),
            F.col('city').alias('zone_id')
        )

    # Шаг 4: считаем расстояние между парами, оставляем только <= 1 км
    pairs_with_distance = group_pairs \
        .join(user_coordinates.alias('u1'), F.col('user_left') == F.col('u1.user_id')) \
        .join(user_coordinates.alias('u2'), F.col('user_right') == F.col('u2.user_id')) \
        .withColumn('d', haversine('u1.lat', 'u1.lon', 'u2.lat', 'u2.lon')) \
        .filter(F.col('d') <= 1) \
        .select(
            F.col('user_left'),
            F.col('user_right'),
            F.col('u1.zone_id').alias('zone_id')  # город привязываем к левому пользователю
        )

    # Шаг 5: список контактов — кому писал или кто писал
    contact_list = events_with_city \
        .filter(
            (F.col('event_type') == 'message') &
            F.col('event.message_from').isNotNull() &
            F.col('event.message_to').isNotNull()
        ) \
        .select(
            F.col('event.message_from').alias('user_id'),
            F.col('event.message_to').alias('contact_id')
        ) \
        .distinct()

    # Шаг 6: убираем пары которые уже переписывались (в обе стороны)
    friends_mart = pairs_with_distance \
        .join(
            contact_list,
            (pairs_with_distance['user_left'] == contact_list['user_id']) &
            (pairs_with_distance['user_right'] == contact_list['contact_id']),
            how='left_anti'
        ) \
        .join(
            contact_list,
            (pairs_with_distance['user_right'] == contact_list['user_id']) &
            (pairs_with_distance['user_left'] == contact_list['contact_id']),
            how='left_anti'
        ) \
        .withColumn('processed_dttm', F.lit(date).cast('date')) \
        .withColumn('local_time', F.current_timestamp())

    friends_mart.write \
        .mode('overwrite') \
        .parquet(f"{output_base_path}/date={date}")


if __name__ == '__main__':
    date             = sys.argv[1]       # дата расчёта, формат YYYY-MM-DD
    depth            = int(sys.argv[2])  # глубина выборки в днях
    events_base_path = sys.argv[3]       # путь к staging/events_with_city в HDFS
    output_base_path = sys.argv[4]       # путь к analytics/friends_mart в HDFS

    spark = SparkSession.builder \
        .appName('friends_mart') \
        .config("spark.executor.memory", "9g") \
        .config("spark.executor.cores", "3") \
        .config("spark.executor.instances", "5") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.cores", "2") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.yarn.am.cores", "1") \
        .config("spark.yarn.am.memory", "1g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.sql.shuffle.partitions", "15") \
        .getOrCreate()

    calculate_friends_mart(
        spark=spark,
        date=date,
        depth=depth,
        events_base_path=events_base_path,
        output_base_path=output_base_path,
    )
