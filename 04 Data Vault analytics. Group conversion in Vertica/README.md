# Аналитика конверсий групп. Data Vault + Vertica.

## Описание задачи

Маркетологам социальной платформы нужно было определить сообщества с наибольшей вовлечённостью для приоритизации рекламных расходов. Цель — рассчитать конверсию группы: доля участников, написавших хотя бы одно сообщение, от общего числа вступивших, — используя DWH на Vertica по методологии Data Vault.

## Что было сделано

- Расширен существующий Airflow DAG (скачивание файлов из S3) для загрузки `group_log.csv`
- Добавлен DDL таблицы `group_log` в STG-слой Vertica с партиционированием по дате, создана новая задача DAG с загрузкой через `COPY ... FROM LOCAL`
- Построен DDS-слой Data Vault: линк `l_user_group_activity` (связь пользователь–группа) и сателлит `s_auth_history` (тип события, временная метка, кто добавил пользователя)
- Написаны идемпотентные миграции `INSERT ... WHERE NOT IN` из STG в DDS с хэш-ключами
- Рассчитана конверсия групп тремя CTE: пользователи с сообщениями / вступившие пользователи, ранжирование по конверсии для 10 старейших групп

## Стек и инструменты

`Vertica` `Apache Airflow` `Python` `SQL` `Data Vault` `S3` `CTE` `DDL`

## Структура папки

- `src/` — исходный код
  - `dags/` — Airflow DAG для скачивания из S3 и загрузки в STG
  - `sql/stg/` — DDL и COPY-скрипты для STG-слоя
  - `sql/dds/` — DDL и INSERT-скрипты для линков и сателлитов Data Vault
  - `sql/cte/` — аналитические CTE-запросы для расчёта конверсии групп
- `PROJECT.md` — подробное пошаговое описание реализации

---

## Ход реализации

# Проект: Аналитические базы данных — конверсия групп в первое сообщение

### Шаг 1. Загрузка файлов из S3

**DAG:** `src/dags/dag_s3_load_files.py`

DAG был реализован в процессе прохождения спринта. В рамках проекта в список `bucket_files` добавлен новый файл `group_log.csv`.

```python
bucket_files = ['users.csv', 'groups.csv', 'dialogs.csv', 'group_log.csv']
```

DAG подключается к бакету `sprint6` в Yandex Object Storage (S3-совместимый) через `boto3` и скачивает каждый файл в папку `/src/data/` внутри контейнера:

```python
s3_client = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

for file in bucket_files:
    local_path = f'/src/data/{file}'
    s3_client.download_file(Bucket='sprint6', Key=file, Filename=local_path)
```

После загрузки `BashOperator` выводит первые 10 строк каждого файла для проверки.

**Цепочка задач:**
```
fetch_s3_files >> print_10_lines_of_each
```

---

### Шаги 2–3. Создание таблицы STG и загрузка данных

**DAG:** `src/dags/dag_s3_infill_STG.py`

DAG также был реализован в процессе спринта. В рамках проекта в него были добавлены:
- DDL таблицы `group_log` в таске `t_create_STG_tables`
- Новый таск `t_infill_group_log_STG` для загрузки данных

#### DDL таблицы `group_log` (STG-слой)

**Файл:** `src/sql/stg/group_log_ddl.sql`

```sql
DROP TABLE IF EXISTS VT260224AD30FB__STAGING.group_log;

CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.group_log
(
    group_id     INT         NOT NULL PRIMARY KEY,
    user_id      INT         NOT NULL,
    user_id_from INT,           -- NULL, если пользователь вступил сам
    event        VARCHAR(6),    -- 'create', 'add', 'leave'
    datetime     DATETIME
)
ORDER BY group_id
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2)
;
```

Партиционирование по дате ускоряет запросы с фильтрацией по времени. `user_id_from` допускает `NULL` — поле заполняется только когда пользователя пригласил другой участник.

#### Загрузка данных в STG через DAG

DDL добавлен в существующий таск `t_create_STG_tables`, новый таск загружает данные:

```python
@task()
def t_infill_group_log_STG():
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
            COPY VT260224AD30FB__STAGING.group_log (group_id, user_id, user_id_from, event, datetime)
            FROM LOCAL '/src/data/group_log.csv'
            DELIMITER ','
            SKIP 1
            NULL AS ''
            REJECTMAX 100
            REJECTED DATA AS TABLE VT260224AD30FB__STAGING.group_log_rej
            ;
        """)
```

Ключевые параметры `COPY`:
- `NULL AS ''` — пустые строки в CSV трактуются как NULL (важно для `user_id_from`)
- `REJECTMAX 100` — допускает до 100 ошибочных строк без прерывания загрузки
- `REJECTED DATA AS TABLE` — отклонённые строки сохраняются в таблицу `group_log_rej` для анализа

**Цепочка задач после добавления:**
```
t_create_STG_tables >> t_infill_users_STG >> t_infill_groups_STG >> t_infill_dialogs_STG >> t_infill_group_log_STG
```

---

### Шаг 4. Создание линка `l_user_group_activity` в DDS

**Файл:** `src/sql/dds/l_user_group_activity_ddl.sql`

Линк (link) в методологии Data Vault связывает два хаба — пользователей и группы. Он фиксирует сам факт связи между сущностями без атрибутов.

```sql
CREATE TABLE IF NOT EXISTS VT260224AD30FB__DWH.l_user_group_activity
(
    hk_l_user_group_activity BIGINT PRIMARY KEY,
    hk_user_id               BIGINT NOT NULL
        CONSTRAINT fk_l_user_group_activity_h_users
            REFERENCES VT260224AD30FB__DWH.h_users (hk_user_id),
    hk_group_id              BIGINT NOT NULL
        CONSTRAINT fk_l_user_group_activity_h_groups
            REFERENCES VT260224AD30FB__DWH.h_groups (hk_group_id),
    load_dt                  DATETIME,
    load_src                 VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)
;
```

- `SEGMENTED BY ... ALL NODES` — данные равномерно распределяются по всем узлам кластера Vertica для параллельной обработки
- `hk_l_user_group_activity` — хэш-ключ, вычисляемый как `hash(group_id, user_id)`

---

### Шаг 5. Миграция данных в линк

**Файл:** `src/sql/dds/l_user_group_activity_insert.sql`

```sql
INSERT INTO VT260224AD30FB__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
    hash(hg.group_id, hu.user_id) AS hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    now()                         AS load_dt,
    's3'                          AS load_src
FROM VT260224AD30FB__STAGING.group_log AS sgl
LEFT JOIN VT260224AD30FB__DWH.h_users  hu ON sgl.user_id  = hu.user_id
LEFT JOIN VT260224AD30FB__DWH.h_groups hg ON sgl.group_id = hg.group_id
WHERE hash(hg.group_id, hu.user_id) NOT IN (
    SELECT hk_l_user_group_activity FROM VT260224AD30FB__DWH.l_user_group_activity
)
;
```

Ключевые решения:
- `DISTINCT` — в `group_log` может быть несколько событий для одной пары (user, group), в линк пишем только уникальные связи
- `hash(hg.group_id, hu.user_id)` — детерминированный хэш-ключ: один и тот же хэш всегда означает одну и ту же связь
- `WHERE ... NOT IN (SELECT ...)` — идемпотентность: при повторном запуске уже загруженные записи не дублируются

---

### Шаг 6. Создание и наполнение сателлита `s_auth_history`

**DDL:** `src/sql/dds/s_auth_history_ddl.sql`

Сателлит (satellite) хранит атрибуты и историю изменений связи из линка. В отличие от линков и хабов, у сателлитов **нет первичного ключа**.

```sql
CREATE TABLE IF NOT EXISTS VT260224AD30FB__DWH.s_auth_history
(
    hk_l_user_group_activity BIGINT NOT NULL
        CONSTRAINT fk_s_auth_history_l_user_group_activity
            REFERENCES VT260224AD30FB__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from             BIGINT,       -- кто добавил; NULL = сам вступил
    event                    VARCHAR(6),   -- 'create', 'add', 'leave'
    event_dt                 TIMESTAMP(0),
    load_dt                  DATETIME,
    load_src                 VARCHAR(20)
)
;
```

#### Миграция данных в сателлит

**Файл:** `src/sql/dds/s_auth_history_insert.sql`

```sql
INSERT INTO VT260224AD30FB__DWH.s_auth_history (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT
    luga.hk_l_user_group_activity,
    sgl.user_id_from,
    sgl.event,
    sgl.datetime AS event_dt,
    now()        AS load_dt,
    's3'         AS load_src
FROM VT260224AD30FB__STAGING.group_log AS sgl
LEFT JOIN VT260224AD30FB__DWH.h_groups               hg   ON sgl.group_id  = hg.group_id
LEFT JOIN VT260224AD30FB__DWH.h_users                hu   ON sgl.user_id   = hu.user_id
LEFT JOIN VT260224AD30FB__DWH.l_user_group_activity  luga ON hg.hk_group_id = luga.hk_group_id
                                                         AND hu.hk_user_id  = luga.hk_user_id
WHERE luga.hk_l_user_group_activity NOT IN (
    SELECT hk_l_user_group_activity FROM VT260224AD30FB__DWH.s_auth_history
)
;
```

Логика JOIN-цепочки:
1. Из `group_log` через `h_groups` получаем `hk_group_id`
2. Из `group_log` через `h_users` получаем `hk_user_id`
3. Через оба хэш-ключа находим нужную запись в `l_user_group_activity` и берём её `hk_l_user_group_activity`

---

### Шаг 7. Расчёт конверсии — CTE для ответа бизнесу

#### CTE 7.1: `user_group_messages` — пользователи, написавшие сообщения

**Файл:** `src/sql/cte/user_group_messages.sql`

```sql
WITH user_group_messages AS (
    SELECT
        lgd.hk_group_id,
        count(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM VT260224AD30FB__DWH.l_groups_dialogs lgd
    JOIN VT260224AD30FB__DWH.h_dialogs        hd  ON hd.hk_message_id  = lgd.hk_message_id
    JOIN VT260224AD30FB__DWH.l_user_message   lum ON lum.hk_message_id = hd.hk_message_id
    GROUP BY lgd.hk_group_id
)
SELECT hk_group_id, cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages
LIMIT 10;
```

Маршрут по Data Vault:
- `l_groups_dialogs` — связывает группы с сообщениями
- `h_dialogs` — хаб сообщений
- `l_user_message` — связывает сообщения с пользователями
- `DISTINCT` гарантирует подсчёт уникальных пользователей, а не сообщений

#### CTE 7.2: `user_group_log` — пользователи, вступившие в группу

**Файл:** `src/sql/cte/user_group_log.sql`

```sql
WITH user_group_log AS (
    SELECT
        lga.hk_group_id,
        count(DISTINCT sah.user_id_from) AS cnt_added_users
    FROM VT260224AD30FB__DWH.s_auth_history        sah
    JOIN VT260224AD30FB__DWH.l_user_group_activity lga ON lga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    WHERE sah.event = 'add'
    GROUP BY lga.hk_group_id
)
SELECT hk_group_id, cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users
LIMIT 10;
```

- Фильтр `WHERE sah.event = 'add'` — учитываем только факты вступления в группу
- Данные берутся из сателлита `s_auth_history`, созданного на шаге 6

#### CTE 7.3: Итоговый расчёт `group_conversion`

**Файл:** `src/sql/cte/group_conversion.sql`

```sql
WITH user_group_log AS ( ... )
, user_group_messages AS ( ... )
, oldest_groups AS (
    SELECT hk_group_id
    FROM VT260224AD30FB__DWH.h_groups
    ORDER BY registration_dt ASC
    LIMIT 10
)

SELECT
    og.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users::FLOAT AS group_conversion
FROM oldest_groups og
LEFT JOIN user_group_log      ugl ON og.hk_group_id = ugl.hk_group_id
LEFT JOIN user_group_messages ugm ON og.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC
;
```

- `oldest_groups` — ограничение выборки 10 самыми старыми группами по `registration_dt`
- `::FLOAT` — явное приведение типа для получения дробного результата деления
- `LEFT JOIN` — если для группы нет данных в одном из CTE, она всё равно попадает в результат (с NULL)
- Сортировка `ORDER BY group_conversion DESC` ставит наиболее конверсионные группы наверх

---

| Шаг | Что сделано | Файл |
|-----|-------------|------|
| 1 | Добавлен `group_log.csv` в DAG скачивания из S3 | `dag_s3_load_files.py` |
| 2 | DDL таблицы `group_log` добавлен в DAG STG | `dag_s3_infill_STG.py` |
| 3 | Таск загрузки `group_log` добавлен в DAG STG | `dag_s3_infill_STG.py` |
| 4 | Создан линк `l_user_group_activity` в DDS | `l_user_group_activity_ddl.sql` |
| 5 | Миграция данных в линк из STG | `l_user_group_activity_insert.sql` |
| 6 | Создан сателлит `s_auth_history`, миграция данных | `s_auth_history_ddl.sql`, `s_auth_history_insert.sql` |
| 7 | CTE для расчёта конверсии по 10 старейшим группам | `group_conversion.sql` |
