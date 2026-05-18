import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def input_paths(date: str, depth: int, path: str) -> list:
    """Генерирует список путей к партициям за последние depth дней от date."""
    dates = [(datetime.strptime(date, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(depth)]
    return [f"{path}/date={d}" for d in dates]


def calculate_zones_mart(spark, date: str, depth: int, events_base_path: str, output_base_path: str):
    """
    Строит витрину по зонам zones_mart.
    Поля: zone_id, year, month, week, week_message_cnt, week_reaction_cnt,
          week_subscription_cnt, month_message_cnt, month_reaction_cnt,
          month_subscription_cnt, week_reg_cnt, month_reg_cnt
    """

    # Чтение данных за нужный период
    events_with_city = spark.read \
        .option("basePath", events_base_path) \
        .parquet(*input_paths(date, depth, events_base_path))

    # Добавляем временные атрибуты для группировки
    zones_events = events_with_city \
        .withColumn('year', F.year('date')) \
        .withColumn('month', F.month('date')) \
        .withColumn('week', F.weekofyear('date'))

    # Подсчёт событий за неделю по типам
    week_zones_events = zones_events.groupBy('id', 'year', 'month', 'week') \
        .agg(
            F.count(F.when(F.col('event_type') == 'message', F.lit(1))).alias('week_message_cnt'),
            F.count(F.when(F.col('event_type') == 'reaction', F.lit(1))).alias('week_reaction_cnt'),
            F.count(F.when(F.col('event_type') == 'subscription', F.lit(1))).alias('week_subscription_cnt')
        )

    # Подсчёт событий за месяц по типам
    month_zones_events = zones_events.groupBy('id', 'year', 'month') \
        .agg(
            F.count(F.when(F.col('event_type') == 'message', F.lit(1))).alias('month_message_cnt'),
            F.count(F.when(F.col('event_type') == 'reaction', F.lit(1))).alias('month_reaction_cnt'),
            F.count(F.when(F.col('event_type') == 'subscription', F.lit(1))).alias('month_subscription_cnt')
        )

    # Регистрации = первое сообщение пользователя (по всей истории)
    w_reg = Window.partitionBy('event.message_from').orderBy(F.col('date').asc())

    registrations = zones_events \
        .filter(F.col('event.message_from').isNotNull()) \
        .withColumn('rnk', F.row_number().over(w_reg)) \
        .filter(F.col('rnk') == 1)

    week_zones_reg = registrations.groupBy('id', 'year', 'month', 'week') \
        .agg(F.count('*').alias('week_reg_cnt'))

    month_zones_reg = registrations.groupBy('id', 'year', 'month') \
        .agg(F.count('*').alias('month_reg_cnt'))

    zones_reg = week_zones_reg.join(month_zones_reg, on=['id', 'year', 'month'], how='inner')

    # Финальный join: регистрации left — в некоторых зонах/неделях может не быть новых пользователей
    zones_mart = week_zones_events \
        .join(month_zones_events, on=['id', 'year', 'month'], how='inner') \
        .join(zones_reg, on=['id', 'year', 'month', 'week'], how='left') \
        .withColumnRenamed('id', 'zone_id')

    zones_mart.write \
        .mode('overwrite') \
        .parquet(f"{output_base_path}/date={date}")


if __name__ == '__main__':
    date             = sys.argv[1]        # дата расчёта, формат YYYY-MM-DD
    depth            = int(sys.argv[2])   # глубина выборки в днях
    events_base_path = sys.argv[3]        # путь к staging/events_with_city в HDFS
    output_base_path = sys.argv[4]        # путь к analytics/zones_mart в HDFS

    spark = SparkSession.builder \
        .appName('zones_mart') \
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

    calculate_zones_mart(
        spark=spark,
        date=date,
        depth=depth,
        events_base_path=events_base_path,
        output_base_path=output_base_path,
    )
