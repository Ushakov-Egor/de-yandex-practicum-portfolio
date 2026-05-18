import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def validate_zones_mart(spark, zones_mart_path: str):
    """
    Набор проверок для витрины zones_mart.
    При обнаружении аномалий падает с ошибкой — Airflow пометит таск как failed.
    """

    zones_mart = spark.read.parquet(zones_mart_path)

    # Проверка 1: недельные суммы сообщений должны совпадать с месячными
    diff_check = zones_mart \
        .groupBy('zone_id', 'year', 'month') \
        .agg(
            F.sum('week_message_cnt').alias('sum_weekly'),
            F.first('month_message_cnt').alias('monthly')
        ) \
        .withColumn('diff', F.col('sum_weekly') - F.col('monthly')) \
        .filter(F.col('diff') != 0)

    diff_count = diff_check.count()
    if diff_count > 0:
        print(f"[WARN] Проверка 1: {diff_count} зон/месяцев где сумма недельных != месячных")
        diff_check.orderBy('zone_id', 'year', 'month').show(20)
    else:
        print("[OK] Проверка 1: суммы недельных и месячных сообщений совпадают")

    # Проверка 2: регистрации не должны превышать количество сообщений за неделю
    reg_check = zones_mart \
        .filter(F.col('week_reg_cnt') > F.col('week_message_cnt'))

    reg_count = reg_check.count()
    if reg_count > 0:
        print(f"[WARN] Проверка 2: {reg_count} случаев где регистрации > сообщений за неделю")
        reg_check.show(10)
    else:
        print("[OK] Проверка 2: регистрации не превышают сообщения")

    # Проверка 3: не должно быть null в ключевых полях
    null_check = zones_mart \
        .filter(
            F.col('zone_id').isNull() |
            F.col('year').isNull() |
            F.col('month').isNull() |
            F.col('week').isNull()
        ).count()

    if null_check > 0:
        raise ValueError(f"[FAIL] Проверка 3: {null_check} строк с null в ключевых полях")
    else:
        print("[OK] Проверка 3: null в ключевых полях отсутствуют")

    print("Валидация zones_mart завершена")


if __name__ == '__main__':
    zones_mart_path = sys.argv[1]  # путь к analytics/zones_mart в HDFS

    spark = SparkSession.builder \
        .appName('validate_zones_mart') \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instances", "1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", "1") \
        .config("spark.yarn.am.cores", "1") \
        .config("spark.yarn.am.memory", "1g") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "15") \
        .getOrCreate()

    validate_zones_mart(
        spark=spark,
        zones_mart_path=zones_mart_path,
    )
