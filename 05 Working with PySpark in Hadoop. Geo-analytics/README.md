# Геоаналитика социальной сети. PySpark + Hadoop

## Описание задачи

Социальная сеть расширяется на Австралию и планирует монетизацию через геотаргетированную рекламу и систему рекомендации друзей. В проекте обновлена структура Data Lake в HDFS и построены три аналитические витрины: по пользователям (текущий и домашний город, история перемещений), по зонам (активность по городам Австралии), и рекомендации друзей (пары из одного канала, не переписывавшихся, на расстоянии ≤ 1 км).

## Что было сделано

- Написан скрипт определения ближайшего города для каждого события по формуле Haversine через crossJoin с 24 городами Австралии и ранжированием по расстоянию внутри оконной функции
- Построена витрина пользователей: актуальный город (последнее сообщение), локальное время по таймзоне города, домашний город через технику «острова и промежутки» (27+ дней подряд), история перемещений через `collect_list` по оконной функции
- Построена витрина зон: счётчики событий по неделям и месяцам, количество регистраций (первое сообщение пользователя) с разбивкой по городу
- Построена витрина рекомендаций: пары пользователей из одного канала, не переписывавшихся, с расстоянием ≤ 1 км; оптимизирован join — пары строятся только внутри каналов, не crossJoin
- Реализован DAG валидации zones_mart: три проверки качества данных (консистентность week/month, корректность счётчиков регистраций, отсутствие null в ключевых полях)

## Стек и инструменты

`Apache Spark` `PySpark` `YARN` `HDFS` `Apache Airflow` `Python` `Оконные функции`

## Структура папки

- `src/` — исходный код
  - `dags/main_dag.py` — основной DAG пайплайна (ежедневно в полночь)
  - `dags/validate_dag.py` — DAG валидации витрины zones_mart
  - `scripts/events_with_city.py` — присвоение города каждому событию
  - `scripts/users_mart.py` — витрина по пользователям
  - `scripts/zones_mart.py` — витрина по зонам
  - `scripts/friends_mart.py` — витрина рекомендаций друзей
  - `scripts/validate_zones_mart.py` — проверки качества данных
- `geo.csv` — координаты 24 городов Австралии

---

## Ход реализации

## Исходные данные

### События социальной сети

**Путь в HDFS:** `/user/master/data/geo/events`

Данные партиционированы по `date` и `event_type`. Каждое событие содержит вложенную структуру `event` с полями, специфичными для типа события:

| Поле | Описание |
|------|----------|
| `event_type` | Тип события: `message`, `reaction`, `subscription` |
| `lat`, `lon` | Координаты события (могут быть null) |
| `date` | Дата события |
| `event.message_from` | ID отправителя (для сообщений) |
| `event.message_to` | ID получателя (для личных сообщений) |
| `event.message_group` | ID группового канала (для групповых сообщений) |
| `event.datetime` | Временная метка (для реакций и подписок) |
| `event.message_ts` | Временная метка сообщения |

> Поля `event.datetime` и `event.message_ts` взаимоисключают друг друга — в зависимости от типа события заполнено одно из них. При построении витрин используется `F.coalesce(F.col('event.datetime'), F.col('event.message_ts'))`.

### Справочник городов

**Путь в HDFS:** `/user/ushakovego/data/de-project-sprint-7/geo.csv`

CSV-файл с 24 городами Австралии, разделитель `;`. Важная особенность: координаты записаны с запятой в качестве десятичного разделителя (`-33,865`), поэтому при чтении применяется очистка:

```python
geo_modified = geo \
    .withColumn('lat', F.regexp_replace('lat', ',', '.').cast('double')) \
    .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('double'))
```

---

## Структура Data Lake в HDFS

```
/user/ushakovego/data/de-project-sprint-7/
├── staging/
│   └── events_with_city/            # события с присвоенным городом
│       └── date=YYYY-MM-DD/
│           └── event_type=.../
└── analytics/
    ├── users_mart/                  # витрина по пользователям
    │   └── date=YYYY-MM-DD/
    ├── zones_mart/                  # витрина по зонам
    │   └── date=YYYY-MM-DD/
    └── friends_mart/                # витрина рекомендации друзей
        └── date=YYYY-MM-DD/
```

Паттерн `staging → analytics`: сначала определяется город для каждого события и результат сохраняется в `staging/events_with_city`. Все три аналитические витрины читают именно этот слой — Haversine считается один раз.

---

## Шаг 1: Определение города (events_with_city)

**Скрипт:** `src/scripts/events_with_city.py`

Для каждого события определяется ближайший австралийский город по формуле Haversine. Подход: `crossJoin` событий со справочником городов (24 города — допустимо), затем ранжирование по расстоянию внутри окна `(lat, lon, date, event_type)`.

### Формула Haversine

```python
def haversine(lat1, lon1, lat2, lon2):
    lat1_r = F.radians(F.col(lat1))
    lat2_r = F.radians(F.col(lat2))
    lon1_r = F.radians(F.col(lon1))
    lon2_r = F.radians(F.col(lon2))

    return 2 * 6371 * F.asin(
        F.sqrt(
            F.pow(F.sin((lat2_r - lat1_r) / 2), 2)
            + F.cos(lat1_r) * F.cos(lat2_r)
            * F.pow(F.sin((lon2_r - lon1_r) / 2), 2)
        )
    )
```

### Выбор ближайшего города

```python
window = Window.partitionBy('lat', 'lon', 'date', 'event_type').orderBy(F.col('d').asc())

events_with_city = events_cities \
    .withColumn('rnk', F.row_number().over(window)) \
    .filter(F.col('rnk') == 1) \
    .drop('city_lat', 'city_lng', 'd', 'rnk')
```

Результат сохраняется в `staging/events_with_city` с партиционированием по `date` и `event_type`.

### Схема результата

```
root
 |-- event: struct
 |-- lat: double
 |-- lon: double
 |-- id: string        # ID города из geo.csv
 |-- city: string      # название города
 |-- date: date
 |-- event_type: string
```

---

## Шаг 2: Витрина пользователей (users_mart)

**Скрипт:** `src/scripts/users_mart.py`

**Схема:**
```
root
 |-- user_id: long
 |-- act_city: string
 |-- local_time: timestamp
 |-- home_city: string
 |-- travel_count: integer
 |-- travel_array: array<string>
 |-- date: date
```

### act_city и local_time

Актуальный город — город последнего сообщения пользователя. Локальное время вычисляется через маппинг города на таймзону и `F.from_utc_timestamp`:

```python
mapping_expr = F.create_map([F.lit(x) for pair in city_timezones.items() for x in pair])

act_city = messages \
    .withColumn('rnk', F.row_number().over(w_last)) \
    .filter(F.col('rnk') == 1) \
    .withColumn('timezone', mapping_expr[F.col('act_city')]) \
    .withColumn('local_time', F.from_utc_timestamp(F.col('datetime_utc'), F.col('timezone')))
```

### travel_array

Список городов в хронологическом порядке посещения. Используется `collect_list` над оконной функцией с сортировкой по дате — это гарантирует порядок элементов (в отличие от `orderBy` + `groupBy`, где Spark не гарантирует сохранение порядка при агрегации):

```python
w_travel = Window.partitionBy('event.message_from').orderBy(F.col('date').asc())

travel = messages \
    .withColumn('travel_array', F.collect_list('city').over(w_travel)) \
    .groupBy(F.col('event.message_from').alias('user_id')) \
    .agg(F.max('travel_array').alias('travel_array')) \
    .withColumn('travel_count', F.size('travel_array'))
```

### home_city: техника Islands and Gaps

Домашний город — последний город, где пользователь непрерывно находился 27 и более дней. Для определения непрерывных серий используется техника «острова и промежутки»:

1. `F.lag('city')` — определяем предыдущий город
2. Помечаем строки, где город изменился (`city_changed = True`)
3. Накапливающая сумма `city_changed` даёт уникальный номер серии (`island_id`)
4. Считаем уникальные дни в каждой серии, фильтруем по `days_count >= 27`
5. Берём последнюю серию (по `last_date`)

```python
w_user = Window.partitionBy('event.message_from').orderBy('date')

messages_with_change = messages \
    .withColumn('prev_city', F.lag('city').over(w_user)) \
    .withColumn('city_changed',
        (F.col('city') != F.col('prev_city')) | F.col('prev_city').isNull()
    ) \
    .withColumn('island_id',
        F.sum(F.col('city_changed').cast('int')).over(w_user)
    )

islands = messages_with_change \
    .groupBy('event.message_from', 'island_id', 'city') \
    .agg(
        F.countDistinct('date').alias('days_count'),
        F.max('date').alias('last_date')
    ) \
    .filter(F.col('days_count') >= 27)
```

> `home_city` может быть `null` — это нормально для пользователей, которые часто меняют город или у которых недостаточно данных в рассматриваемом периоде.

---

## Шаг 3: Витрина зон (zones_mart)

**Скрипт:** `src/scripts/zones_mart.py`

**Схема:**
```
root
 |-- zone_id: string
 |-- year: integer
 |-- month: integer
 |-- week: integer
 |-- week_message_cnt: long
 |-- week_reaction_cnt: long
 |-- week_subscription_cnt: long
 |-- month_message_cnt: long
 |-- month_reaction_cnt: long
 |-- month_subscription_cnt: long
 |-- week_reg_cnt: long
 |-- month_reg_cnt: long
 |-- date: date
```

### Подсчёт событий

Сначала добавляем временные атрибуты, затем группируем отдельно по неделям и месяцам:

```python
week_zones_events = zones_events.groupBy('id', 'year', 'month', 'week') \
    .agg(
        F.count(F.when(F.col('event_type') == 'message', F.lit(1))).alias('week_message_cnt'),
        F.count(F.when(F.col('event_type') == 'reaction', F.lit(1))).alias('week_reaction_cnt'),
        F.count(F.when(F.col('event_type') == 'subscription', F.lit(1))).alias('week_subscription_cnt')
    )
```

### Подсчёт регистраций

Регистрация — первое сообщение пользователя в истории данных. Определяется через `row_number()` с сортировкой по дате по возрастанию:

```python
w_reg = Window.partitionBy('event.message_from').orderBy(F.col('date').asc())

registrations = zones_events \
    .filter(F.col('event.message_from').isNotNull()) \
    .withColumn('rnk', F.row_number().over(w_reg)) \
    .filter(F.col('rnk') == 1)
```

Финальный джойн регистраций к витрине — `left`, так как в некоторых зонах/неделях может не быть новых пользователей.

---

## Шаг 4: Витрина рекомендации друзей (friends_mart)

**Скрипт:** `src/scripts/friends_mart.py`

**Схема:**
```
root
 |-- user_left: long
 |-- user_right: long
 |-- zone_id: string
 |-- processed_dttm: date
 |-- local_time: timestamp
 |-- date: date
```

**Условия для рекомендации:** пара пользователей подписана на один канал, ни разу не переписывалась между собой, и расстояние между ними ≤ 1 км.

### Оптимизация: пары только внутри каналов

Изначально рассматривался подход с `crossJoin` всех пользователей, но он даёт квадратичный рост числа пар. Финальная оптимизация: сначала строим пары только внутри каналов (пользователи, которые уже подписаны на один канал), и только потом считаем расстояние между ними.

```
1. group_users        — все пользователи для каждого канала
2. group_pairs        — уникальные пары внутри каждого канала
3. user_coordinates   — актуальные координаты каждого пользователя
4. pairs_with_dist    — расстояние между парами, фильтр <= 1 км
5. contact_list       — список всех переписок
6. result             — убираем пары, которые уже переписывались
```

### Уникальные пары без дублей

Условие `u1.user_id < u2.user_id` исключает одновременно дубли `(u1, u2)` = `(u2, u1)` и пары пользователя с самим собой:

```python
group_pairs = group_users.alias('u1') \
    .join(group_users.alias('u2'), on='group_id') \
    .filter(F.col('u1.user_id') < F.col('u2.user_id')) \
    .select('group_id', F.col('u1.user_id').alias('user_left'), F.col('u2.user_id').alias('user_right')) \
    .distinct()
```

### Исключение переписывавшихся пользователей

Два `left_anti` джойна — проверяем обе стороны переписки:

```python
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
    )
```

---

## Валидация данных (zones_mart)

**Скрипт:** `src/scripts/validate_zones_mart.py`
**DAG:** `src/dags/validate_dag.py`

Запускается ежедневно в 00:30 (через 30 минут после основного пайплайна) и выполняет три проверки:

| # | Проверка | Поведение при нарушении |
|---|----------|------------------------|
| 1 | Сумма `week_message_cnt` за все недели месяца == `month_message_cnt` | Предупреждение (WARN), вывод аномалий |
| 2 | `week_reg_cnt` не превышает `week_message_cnt` | Предупреждение (WARN), вывод аномалий |
| 3 | Нет `null` в ключевых полях (`zone_id`, `year`, `month`, `week`) | Ошибка (FAIL) — таск падает, Airflow помечает как failed |

Проверки 1 и 2 информационные — они выводят предупреждения, но не блокируют пайплайн. Проверка 3 критическая: наличие `null` в ключевых полях указывает на проблему в логике построения витрины.

---

## Оркестрация (Apache Airflow)

### Основной DAG: `geo_analytics_pipeline`

**Файл:** `src/dags/main_dag.py`
**Расписание:** `0 0 * * *` (ежедневно в полночь)

Таски выполняются **последовательно** из-за ограничений ресурсов YARN-кластера:

```
events_with_city >> users_mart >> zones_mart >> friends_mart
```

Все пути и параметры вынесены в переменные верхнего уровня DAG и передаются в скрипты через `application_args`:

```python
SCRIPTS_PATH = '/lessons/scripts'
EVENTS_SOURCE_PATH = '/user/master/data/geo/events'
GEO_CSV_PATH = '/user/ushakovego/data/de-project-sprint-7/geo.csv'
STAGING_PATH = '/user/ushakovego/data/de-project-sprint-7/staging/events_with_city'
DEPTH = '15'          # глубина выборки в днях
DATE = '2022-06-21'   # дата актуальных данных
```

### DAG валидации: `validate_zones_mart`

**Файл:** `src/dags/validate_dag.py`
**Расписание:** `30 0 * * *` (ежедневно в 00:30)

Запускается через 30 минут после основного пайплайна, читает уже записанные данные `zones_mart` и проверяет их качество.

### Настройки Airflow

Для работы DAG-ов необходимо создать:
- **Соединение** `yarn_spark` типа `Spark` с указанием адреса YARN-кластера
- **Переменные** (при необходимости) для хранения путей

---

## Ресурсы YARN-кластера

Основные таски используют повышенные ресурсы (операции crossJoin и Window):
- `spark.executor.memory=9g`, `spark.executor.cores=3`, `spark.executor.instances=5`
- `spark.driver.memory=4g`, `spark.driver.cores=2`
- `spark.yarn.am.cores=1`, `spark.yarn.am.memory=1g`

Валидатор работает с уже агрегированными данными и требует меньше ресурсов:
- `spark.executor.memory=4g`, `spark.executor.cores=2`, `spark.executor.instances=1`
- `spark.driver.memory=2g`, `spark.driver.cores=1`

> При настройке ресурсов важно следить, чтобы сумма `executor.cores + driver.cores + yarn.am.cores` не превышала лимит vCores кластера.
