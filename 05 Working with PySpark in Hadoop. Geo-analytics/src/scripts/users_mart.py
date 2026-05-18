import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def input_paths(date: str, depth: int, path: str) -> list:
    """Генерирует список путей к партициям за последние depth дней от date."""
    dates = [(datetime.strptime(date, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(depth)]
    return [f"{path}/date={d}" for d in dates]


def calculate_users_mart(spark, date: str, depth: int, events_base_path: str, output_base_path: str):
    """
    Строит витрину пользователей users_mart.
    Поля: user_id, act_city, local_time, home_city, travel_count, travel_array
    """

    # Маппинг австралийских городов на таймзоны
    city_timezones = {
        'Sydney': 'Australia/Sydney',
        'Melbourne': 'Australia/Melbourne',
        'Brisbane': 'Australia/Brisbane',
        'Perth': 'Australia/Perth',
        'Adelaide': 'Australia/Adelaide',
        'Gold Coast': 'Australia/Brisbane',
        'Cranbourne': 'Australia/Melbourne',
        'Canberra': 'Australia/Sydney',
        'Newcastle': 'Australia/Sydney',
        'Wollongong': 'Australia/Sydney',
        'Geelong': 'Australia/Melbourne',
        'Hobart': 'Australia/Hobart',
        'Townsville': 'Australia/Brisbane',
        'Ipswich': 'Australia/Brisbane',
        'Cairns': 'Australia/Brisbane',
        'Toowoomba': 'Australia/Brisbane',
        'Darwin': 'Australia/Darwin',
        'Ballarat': 'Australia/Melbourne',
        'Bendigo': 'Australia/Melbourne',
        'Launceston': 'Australia/Hobart',
        'Mackay': 'Australia/Brisbane',
        'Rockhampton': 'Australia/Brisbane',
        'Maitland': 'Australia/Sydney',
        'Bunbury': 'Australia/Perth'
    }

    # Чтение данных за нужный период
    events_with_cities = spark.read \
        .option("basePath", events_base_path) \
        .parquet(*input_paths(date, depth, events_base_path))

    # Только сообщения — у них есть координаты и message_from
    messages = events_with_cities.filter(F.col('event_type') == 'message')

    mapping_expr = F.create_map([F.lit(x) for pair in city_timezones.items() for x in pair])

    # act_city + local_time: город и время последнего сообщения пользователя
    w_last = Window.partitionBy('event.message_from').orderBy(F.col('date').desc())

    act_city = messages \
        .withColumn('rnk', F.row_number().over(w_last)) \
        .filter(F.col('rnk') == 1) \
        .select(
            F.col('event.message_from').alias('user_id'),
            F.col('city').alias('act_city'),
            F.col('date'),
            # datetime и message_ts взаимоисключают друг друга — берём непустое
            F.coalesce(F.col('event.datetime'), F.col('event.message_ts')).alias('datetime_utc')
        ) \
        .withColumn('timezone', mapping_expr[F.col('act_city')]) \
        .withColumn('local_time', F.from_utc_timestamp(F.col('datetime_utc'), F.col('timezone')))

    # travel_array: список городов в хронологическом порядке посещения
    w_travel = Window.partitionBy('event.message_from').orderBy(F.col('date').asc())

    travel = messages \
        .withColumn('travel_array', F.collect_list('city').over(w_travel)) \
        .groupBy(F.col('event.message_from').alias('user_id')) \
        .agg(F.max('travel_array').alias('travel_array')) \
        .withColumn('travel_count', F.size('travel_array'))

    # home_city: последний город где пользователь был непрерывно 27+ дней
    # Техника islands and gaps — помечаем начало каждой новой серии городов
    w_user = Window.partitionBy('event.message_from').orderBy('date')

    messages_with_change = messages \
        .withColumn('prev_city', F.lag('city').over(w_user)) \
        .withColumn('city_changed',
            (F.col('city') != F.col('prev_city')) | F.col('prev_city').isNull()
        ) \
        .withColumn('island_id',
            F.sum(F.col('city_changed').cast('int')).over(w_user)
        )

    # Считаем уникальные дни в каждой непрерывной серии
    islands = messages_with_change \
        .groupBy(
            F.col('event.message_from').alias('user_id'),
            F.col('island_id'),
            F.col('city')
        ) \
        .agg(
            F.countDistinct('date').alias('days_count'),
            F.max('date').alias('last_date')
        ) \
        .filter(F.col('days_count') >= 27)

    # Берём последнюю серию с >=27 днями
    w_home = Window.partitionBy('user_id').orderBy(F.col('last_date').desc())

    home_city = islands \
        .withColumn('rnk', F.row_number().over(w_home)) \
        .filter(F.col('rnk') == 1) \
        .select('user_id', F.col('city').alias('home_city'))

    # Финальный join: home_city и travel могут быть null — это нормально
    users_mart = act_city \
        .select('user_id', 'act_city', 'local_time') \
        .join(home_city, on='user_id', how='left') \
        .join(travel.select('user_id', 'travel_count', 'travel_array'), on='user_id', how='left')

    users_mart.write \
        .mode('overwrite') \
        .parquet(f"{output_base_path}/date={date}")


if __name__ == '__main__':
    date             = sys.argv[1]        # дата расчёта, формат YYYY-MM-DD
    depth            = int(sys.argv[2])   # глубина выборки в днях
    events_base_path = sys.argv[3]        # путь к staging/events_with_city в HDFS
    output_base_path = sys.argv[4]        # путь к analytics/users_mart в HDFS

    spark = SparkSession.builder \
        .appName('users_mart') \
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

    calculate_users_mart(
        spark=spark,
        date=date,
        depth=depth,
        events_base_path=events_base_path,
        output_base_path=output_base_path,
    )
