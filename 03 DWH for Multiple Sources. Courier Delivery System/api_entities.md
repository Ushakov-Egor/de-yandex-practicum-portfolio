# Проектная работа: DWH для расчётов с курьерами

## Постановка задачи

Необходимо построить витрину данных `cdm.dm_courier_report` для расчёта ежемесячных выплат курьерам. Данные о заказах уже хранятся в DWH (из MongoDB), данные курьерской службы (курьеры, доставки) необходимо загрузить из HTTP API и связать с существующими данными.

---

## 1. Анализ источника: API курьерской службы


### Базовый URL и заголовки

```
BASE URL: https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net
Заголовки:
  X-Nickname: <никнейм>
  X-Cohort:   <номер когорты>
  X-API-KEY:  25c27781-8fde-4b30-a22e-524044a7580f
```

### Используемые эндпоинты

| Эндпоинт | Метод | Назначение |
|---|---|---|
| `/couriers` | GET | Список курьеров (`_id`, `name`) |
| `/deliveries` | GET | Список доставок с деталями |

> `/restaurants` API не используется — данные о ресторанах уже загружаются из MongoDB в существующих DAG-ах спринта.

### Параметры пагинации (одинаковы для всех эндпоинтов)

| Параметр | Описание |
|---|---|
| `sort_field` | Поле сортировки (`id` / `name` / `_id` / `date`) |
| `sort_direction` | Направление сортировки (`asc` / `desc`) |
| `limit` | Размер страницы (макс. 50) |
| `offset` | Смещение для пагинации |

### Поля ответа `/deliveries`

| Поле | Используется | Назначение |
|---|---|---|
| `order_id` | ✅ | Связь с `dds.dm_orders` |
| `order_ts` | ✅ | Дата заказа — checkpoint инкрементальной загрузки |
| `delivery_id` | ✅ | Натуральный ключ доставки |
| `courier_id` | ✅ | Связь с `dds.dm_couriers` |
| `address` | ✅ | Адрес → `dds.dm_delivery_addresses` |
| `delivery_ts` | ✅ | Дата доставки |
| `rate` | ✅ | Рейтинг для расчёта выплаты |
| `sum` | ❌ | Не используется — сумма берётся из MongoDB |
| `tip_sum` | ✅ | Чаевые курьеру |

---

## 2. Проектирование слоёв DWH

### Общая схема потока данных

```
API курьерской службы
        │
        ▼
┌───────────────────────────────────┐
│           STG (сырые данные)      │
│  deliverysystem_couriers          │
│  deliverysystem_deliveries        │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│     DDS (детальный слой,          │
│          модель «снежинка»)       │
│                                   │
│  [существующие таблицы]           │
│  dm_orders, dm_products,          │
│  dm_restaurants, dm_users,        │
│  dm_timestamps, fct_product_sales │
│                                   │
│  [новые таблицы]                  │
│  dm_couriers                      │
│  dm_delivery_addresses            │
│  dm_deliveries                    │
│  fct_courier_deliveries           │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│     CDM (витрины)                 │
│  dm_courier_report                │
└───────────────────────────────────┘
```

---

## 3. DDL: слой STG

Таблицы хранят сырые данные из API в формате JSON. Структура единая: суррогатный ключ, идентификатор объекта из источника и JSON-тело целиком.

### `stg.deliverysystem_couriers`

```sql
drop table if exists stg.deliverysystem_couriers;
create table if not exists stg.deliverysystem_couriers (
    id           serial primary key,
    object_id    varchar,
    object_value json
);
```

`object_value` содержит: `{"_id": "...", "name": "..."}`.

### `stg.deliverysystem_deliveries`

```sql
drop table if exists stg.deliverysystem_deliveries;
create table if not exists stg.deliverysystem_deliveries (
    id           serial primary key,
    object_id    varchar,
    object_value json
);
```

`object_value` содержит все поля из ответа API: `delivery_id`, `order_id`, `order_ts`, `courier_id`, `address`, `delivery_ts`, `rate`, `tip_sum`.

---

## 4. DDL: слой DDS

### Существующие таблицы (уже были в DWH до проекта)

| Таблица | Источник |
|---|---|
| `dds.dm_orders` | MongoDB (`ordersystem_orders`) |
| `dds.dm_products` | MongoDB (`ordersystem_restaurants`) |
| `dds.dm_restaurants` | MongoDB (`ordersystem_restaurants`) |
| `dds.dm_users` | MongoDB (`ordersystem_users`) |
| `dds.dm_timestamps` | MongoDB (`ordersystem_orders`) |
| `dds.fct_product_sales` | MongoDB + bonus system |

### Новые таблицы (добавлены в рамках проекта)

#### `dds.dm_couriers` — справочник курьеров

```sql
drop table if exists dds.dm_couriers;

create table if not exists dds.dm_couriers (
    id        serial  primary key,
    object_id varchar not null unique,  -- _id из API
    name      varchar not null
);
```

#### `dds.dm_delivery_addresses` — справочник адресов доставки

Нормализация: вместо повторяющейся строки адреса в каждой записи доставки хранится ссылка на справочник.

```sql
drop table if exists dds.dm_delivery_addresses;

create table if not exists dds.dm_delivery_addresses (
    id               serial  primary key,
    delivery_address varchar not null unique
);
```

#### `dds.dm_deliveries` — таблица доставок

Связывает заказы, адреса и курьеров. Содержит временны́е метки заказа и доставки.

```sql
create table dds.dm_deliveries (
    id          serial4   not null,
    delivery_id varchar   not null unique,
    order_id    int4      not null,
    address_id  int4      not null,
    order_ts    timestamp not null,
    delivery_ts timestamp not null,
    courier_id  int4      not null,
    constraint dm_deliveries_pkey    primary key (id),
    constraint fk_deliveries_address foreign key (address_id)
        references dds.dm_delivery_addresses(id),
    constraint fk_deliveries_order   foreign key (order_id)
        references dds.dm_orders(id)
);
```

#### `dds.fct_courier_deliveries` — таблица фактов курьерских доставок

Хранит показатели каждой доставки: рейтинг, сумму заказа (из `fct_product_sales`) и чаевые. Является источником для агрегации в витрину.

```sql
drop table if exists dds.fct_courier_deliveries;

create table if not exists dds.fct_courier_deliveries (
    id           serial primary key,
    courier_id   int4           not null,
    delivery_id  int4           not null unique,
    rate         int2           not null,
    delivery_sum numeric(14, 2) not null,
    tip_sum      numeric(14, 2) not null,
    constraint fk_delivery_details_delivery
        foreign key (delivery_id) references dds.dm_deliveries(id)
);
```

---

## 5. DDL: слой CDM

### `cdm.dm_courier_report` — витрина расчётов с курьерами

```sql
drop table if exists cdm.dm_courier_report;

create table if not exists cdm.dm_courier_report (
    id                   serial         primary key,
    courier_id           varchar        not null,
    courier_name         varchar        not null,
    settlement_year      integer        not null,
    settlement_month     integer        not null,
    orders_count         integer        not null default 0,
    orders_total_sum     numeric(14, 2) not null default 0,
    rate_avg             numeric(3, 2)  not null default 0,
    order_processing_fee numeric(14, 2) not null default 0,
    courier_order_sum    numeric(14, 2) not null default 0,
    courier_tips_sum     numeric(14, 2) not null default 0,
    courier_reward_sum   numeric(14, 2) not null default 0,

    constraint dm_courier_report_settlement_month_check
        check (settlement_month between 1 and 12),
    constraint dm_courier_report_settlement_year_check
        check (settlement_year between 2020 and 2100),
    constraint dm_courier_report_rate_avg_check
        check (rate_avg between 1 and 5)
);
```

### Описание полей витрины и формулы расчёта

| Поле | Описание | Формула |
|---|---|---|
| `courier_id` | ID курьера из источника | `dds.dm_couriers.object_id` |
| `courier_name` | Ф.И.О. курьера | `dds.dm_couriers.name` |
| `settlement_year` | Год отчёта | `extract(year from delivery_ts)` |
| `settlement_month` | Месяц отчёта (1–12) | `extract(month from delivery_ts)` |
| `orders_count` | Количество заказов за месяц | `count(order_id)` |
| `orders_total_sum` | Общая сумма заказов | `sum(delivery_sum)` |
| `rate_avg` | Средний рейтинг курьера | `avg(rate)` |
| `order_processing_fee` | Комиссия компании 25% | `orders_total_sum * 0.25` |
| `courier_order_sum` | Выплата курьеру за доставки | зависит от `rate_avg` |
| `courier_tips_sum` | Сумма чаевых | `sum(tip_sum)` |
| `courier_reward_sum` | Итоговая выплата | `(courier_order_sum + courier_tips_sum) * 0.95` |

### Правила расчёта `courier_order_sum`

| Средний рейтинг `r` | % от суммы заказа |
|---|---|
| `r < 4` | 5% |
| `4 ≤ r < 4.5` | 7% |
| `4.5 ≤ r < 4.9` | 8% |
| `r ≥ 4.9` | 10% |

---

## 6. ETL-процессы: DAG-и

### Общая схема зависимостей между DAG-ами

```
[sprint5_project_load_stg]              каждые 15 мин
  t_load_couriers → t_load_deliveries

[sprint5_dds_dms_loader]                каждые 15 мин
  load_users → load_restaurants → load_timestamps
  → load_products → load_orders → load_fact_task
                                            │
                              ExternalTaskSensor
                                            │
[sprint5_load_dds_delivery_system]      каждые 15 мин
  dm_couriers ──┐
                ├──► dm_deliveries ──► [sensor] ──► fct_courier_deliveries
  dm_addresses ─┘

[sprint5_cdm_report_loader]             каждые 20 мин
  cdm_report_loader  (dm_settlement_report)
            │
  ExternalTaskSensor
            │
[sprint5_load_cdm_courier_report]       каждые 20 мин
  t_load_dm_courier_report
```

---

### DAG 1: `sprint5_project_load_stg` — загрузка STG из API

**Файл:** `src/dags/project_dags/load_source_data_to_stg_dag.py`

Загружает данные из HTTP API в две STG-таблицы (рестораны не нужны — данные о них уже есть в DWH из MongoDB).

**Ключевые особенности реализации:**

- **Пагинация** — цикл `while True` с шагом `offset += 50`, выход при пустом ответе от API.
- **Сортировка** — `sort_field=_id, sort_direction=asc` гарантирует детерминированную пагинацию: один и тот же `offset` всегда пропускает одни и те же уже загруженные записи.
- **Инкрементальная загрузка доставок** — параметр `from` в запросе к `/deliveries` задаётся по checkpoint из `stg.srv_wf_settings`. При первом запуске берётся дата 7 дней назад. После загрузки сохраняется `max(order_ts)` как новый checkpoint.
- **Upsert** — `ON CONFLICT (object_id) DO UPDATE SET object_value = EXCLUDED.object_value` перезаписывает изменившиеся данные без дублирования.

**Классы-загрузчики:**

| Класс | Файл | STG-таблица |
|---|---|---|
| `CouriersLoader` | `stg/couriers_loader.py` | `stg.deliverysystem_couriers` |
| `DeliveriesLoader` | `stg/deliveries_loader.py` | `stg.deliverysystem_deliveries` |

---

### DAG 2: `sprint5_load_dds_delivery_system` — загрузка DDS

**Файл:** `src/dags/project_dags/load_dds_for_deliverysystem_dag.py`

Переносит данные из STG в детальный слой DDS. Каждая задача выполняет SQL-скрипт из `src/sql_scripts/dds/dml/` через loader-класс.

**Порядок выполнения задач:**

```
t_load_dm_couriers ──┐
                     ├──► t_load_dm_deliveries ──► [wait_for_fct_product_sales] ──► t_load_fct_courier_deliveries
t_load_dm_addresses ─┘
```

- `dm_couriers` и `dm_delivery_addresses` выполняются параллельно (нет зависимостей).
- `dm_deliveries` требует оба справочника (JOIN по `courier_id` и `address`).
- `fct_courier_deliveries` дополнительно требует `fct_product_sales` (сумма заказа берётся оттуда, а не из API). `ExternalTaskSensor` ждёт завершения `load_fact_task` из `sprint5_dds_dms_loader`.

**SQL-скрипты:**

| Скрипт | Таблица-приёмник | Стратегия конфликтов |
|---|---|---|
| `infill_dm_couriers.sql` | `dds.dm_couriers` | `ON CONFLICT (object_id) DO UPDATE SET name` |
| `infill_dm_delivery_addresses.sql` | `dds.dm_delivery_addresses` | `ON CONFLICT (delivery_address) DO NOTHING` |
| `infill_dm_deliveries.sql` | `dds.dm_deliveries` | `ON CONFLICT (delivery_id) DO UPDATE SET ...` |
| `infill_fct_courier_deliveries.sql` | `dds.fct_courier_deliveries` | `ON CONFLICT (delivery_id) DO UPDATE SET ...` |

**Паттерн реализации loader-классов:**

Путь к SQL-файлу строится через `Path(__file__).resolve().parents[N] / 'sql_scripts' / ...`, где `N` зависит от глубины вложенности файла. SQL выполняется в одной транзакции, после чего в `dds.srv_wf_settings` сохраняется checkpoint с временем последнего запуска.

---

### DAG 3: `sprint5_load_cdm_courier_report` — заполнение витрины

**Файл:** `src/dags/project_dags/load_cdm_courier_report_dag.py`

Заполняет витрину `cdm.dm_courier_report` агрегированными данными из DDS.

**Зависимость:** `ExternalTaskSensor` ждёт завершения `cdm_report_loader` из `sprint5_cdm_report_loader` (витрина `dm_settlement_report`), после чего запускает заполнение `dm_courier_report`.

**Логика SQL-скрипта** `infill_dm_courier_report.sql`:

- `TRUNCATE ... CASCADE` перед INSERT — полная перезапись при каждом запуске (витрина всегда актуальна).
- GROUP BY `courier_id`, `year`, `month`.
- `CASE WHEN avg(rate) < 4 THEN 0.05 WHEN ... THEN 0.07 ...` — расчёт процента выплаты.
- `courier_reward_sum = (courier_order_sum + courier_tips_sum) * 0.95`.

---

## 7. Структура файлов проекта

```
src/
├── dags/
│   ├── project_dags/
│   │   ├── load_source_data_to_stg_dag.py     # DAG STG
│   │   ├── load_dds_for_deliverysystem_dag.py # DAG DDS
│   │   └── load_cdm_courier_report_dag.py     # DAG CDM
│   ├── stg/
│   │   ├── couriers_loader.py
│   │   └── deliveries_loader.py
│   ├── dds/
│   │   ├── couriers_loader.py
│   │   ├── delivery_addresses_loader.py
│   │   ├── deliveries_loader.py
│   │   └── courier_deliveries_loader.py
│   └── cdm/
│       └── courier_report_loader.py
└── sql_scripts/
    ├── stg/
    │   └── ddl/        # DDL таблиц STG-слоя
    ├── dds/
    │   ├── ddl/        # DDL таблиц DDS-слоя
    │   └── dml/        # DML-скрипты заполнения DDS
    └── cdm/
        ├── ddl/        # DDL витрин CDM-слоя
        └── dml/        # DML-скрипты заполнения CDM
```
