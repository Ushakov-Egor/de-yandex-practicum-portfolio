# Миграция DWH. Витрина удержания клиентов.

## Описание задачи

Платформе e-commerce потребовалось добавить отслеживание статуса заказа (`shipped` / `refunded`) в существующую таблицу фактов продаж и построить новую витрину удержания клиентов. Задача включала живую миграцию данных без потерь, обеспечение backward compatibility пайплайна, расширение ETL новыми задачами DAG и оркестрацию через Apache Airflow.

## Что было сделано

- Выполнена миграция таблицы `mart.f_sales`: создана временная таблица с новым столбцом `status`, перенесены исторические данные со статусом `shipped` по умолчанию, таблицы переименованы
- Обеспечена backward compatibility: если входящий инкремент не содержит поля `status` — оно подставляется автоматически со значением `shipped`
- Строки с возвратами (`refunded`) записываются в `mart.f_sales` с отрицательным `payment_amount`, чтобы корректно учитываться в total revenue
- Создана витрина `mart.f_customer_retention`: DDL, скрипт наполнения и задача DAG `update_f_customer_retention` с пересчётом метрик при каждом запуске
- Реализована идемпотентность пайплайна: каждый этап очищает данные за расчётный период перед записью, что позволяет безопасно перезапускать DAG без дублей
- Оркестрирован полный пайплайн (загрузка в staging → обновление измерений → обновление f_sales → пересчёт витрины удержания) в виде единого DAG с явными зависимостями задач

## Стек и инструменты

`Apache Airflow` `PostgreSQL` `Python` `SQL` `REST API` `Yandex Cloud S3` `DDL` `Миграция данных`

## Структура папки

- `src/` — DAG и SQL-скрипты
  - `sales_mart_dag.py` — Airflow DAG со всеми задачами пайплайна
  - `sql/` — SQL-скрипты:
    - `data_migration_f_sales.sql` — миграция mart.f_sales: добавление столбца status, перенос исторических данных
    - `ddl_mart.f_sales.sql` — DDL таблицы mart.f_sales
    - `ddl_stage.user_order_log.sql` — DDL staging-таблицы
    - `ddl_mart.f_customer_retention.sql` — DDL витрины customer retention
    - `ddl_and_infill_mart.f_customer_retention.sql` — DDL + первичное наполнение витрины
    - `infill_mart.f_customer_retention.sql` — инкрементальное наполнение витрины (используется в DAG)
    - `mart.f_sales.sql` — вставка данных из staging в mart.f_sales
    - `mart.d_item.sql`, `mart.d_customer.sql`, `mart.d_city.sql` — обновление таблиц-измерений
- `COMMENTS_FOR_REVIEWERS.md` — описание реализованных изменений для ревьюера

## Ход реализации

### Этап 1: Миграция данных и добавление статуса заказа

Для учёта статуса заказа (`shipped` / `refunded`) выполнена живая миграция `mart.f_sales` без потери записей:

1. Создана временная таблица `mart.temp_f_sales` с полной структурой исходной таблицы и новым столбцом `status varchar(8) NOT NULL`.
2. Все существующие записи перенесены со статусом `'shipped'` по умолчанию.
3. Исходная таблица `mart.f_sales` удалена, временная переименована в `mart.f_sales`.

Скрипт: [`src/sql/data_migration_f_sales.sql`](src/sql/data_migration_f_sales.sql)

**Backward compatibility — обработка инкрементов без статуса:**

Первый инкремент приходит без поля `status`. В функции `upload_data_to_staging` добавлена проверка:

```python
if 'status' not in df.columns:
    df['status'] = 'shipped'
```

**Учёт возвратов в revenue:**

В скрипте наполнения `mart.f_sales` строки с `refunded` записываются с отрицательным `payment_amount`:

```sql
CASE
    WHEN status = 'refunded' THEN -payment_amount
    ELSE payment_amount
END AS payment_amount
```

---

### Этап 2: Витрина customer retention

Создана витрина `mart.f_customer_retention` для анализа возвращаемости клиентов в разрезе недель и категорий товаров.

Структура витрины:

| Поле | Описание |
|---|---|
| `period_name` | Тип периода (`weekly`) |
| `period_id` | Номер недели (формат `YYYYWW`) |
| `item_id` | Категория товара |
| `new_customers_count` | Клиенты с одним заказом за неделю |
| `returning_customers_count` | Клиенты с двумя и более заказами за неделю |
| `refunded_customer_count` | Клиенты с возвратами за неделю |
| `new_customers_revenue` | Доход с новых клиентов |
| `returning_customers_revenue` | Доход с вернувшихся клиентов |
| `customers_refunded` | Количество возвратов |

Метрики рассчитываются через подзапрос: для каждого клиента и категории товара считается число заказов за неделю (`order_count`), затем клиенты делятся на новых (`order_count <= 1`) и вернувшихся (`order_count > 1`).

Скрипт наполнения: [`src/sql/infill_mart.f_customer_retention.sql`](src/sql/infill_mart.f_customer_retention.sql)

Идемпотентность обеспечивается `TRUNCATE TABLE mart.f_customer_retention CASCADE` перед каждой вставкой — DAG можно безопасно перезапустить без дублей.
