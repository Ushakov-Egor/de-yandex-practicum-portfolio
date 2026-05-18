import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_events_with_city(spark, date: str, depth: int, events_path: str, geo_path: str, output_path: str):
    """
    Для каждого события определяет ближайший город по формуле Haversine.
    Результат сохраняется в HDFS партиционированным по date и event_type.
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
    date_filter = F.col('date').isin(dates)

    events = spark.read.parquet(events_path).filter(date_filter)
    geo = spark.read.option("header", "true").option("sep", ";").csv(geo_path)

    # Чистим geo: запятые → точки, приводим к double
    geo_modified = geo \
        .withColumn('lat', F.regexp_replace('lat', ',', '.').cast('double')) \
        .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('double')) \
        .withColumnRenamed('lat', 'city_lat') \
        .withColumnRenamed('lng', 'city_lng')

    # Cross join: каждое событие × каждый город (24 города — приемлемо)
    # Убираем события без координат — им город не определить
    events_cities = events \
        .crossJoin(geo_modified.select('id', 'city', 'city_lat', 'city_lng')) \
        .filter(F.col('lat').isNotNull() & F.col('lon').isNotNull()) \
        .withColumn('d', haversine(lat1='lat', lon1='lon', lat2='city_lat', lon2='city_lng'))

    # Для каждой уникальной координаты оставляем только ближайший город
    window = Window.partitionBy('lat', 'lon', 'date', 'event_type').orderBy(F.col('d').asc())

    events_with_city = events_cities \
        .withColumn('rnk', F.row_number().over(window)) \
        .filter(F.col('rnk') == 1) \
        .drop('city_lat', 'city_lng', 'd', 'rnk')

    events_with_city.write \
        .mode('overwrite') \
        .partitionBy('date', 'event_type') \
        .parquet(output_path)


if __name__ == '__main__':
    date        = sys.argv[1]        # дата расчёта, формат YYYY-MM-DD
    depth       = int(sys.argv[2])   # глубина выборки в днях
    events_path = sys.argv[3]        # /user/master/data/geo/events
    geo_path    = sys.argv[4]        # /user/ushakovego/data/de-project-sprint-7/geo.csv
    output_path = sys.argv[5]        # /user/ushakovego/data/de-project-sprint-7/staging/events_with_city

    spark = SparkSession.builder \
        .appName('events_with_city') \
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

    calculate_events_with_city(
        spark=spark,
        date=date,
        depth=depth,
        events_path=events_path,
        geo_path=geo_path,
        output_path=output_path,
    )
