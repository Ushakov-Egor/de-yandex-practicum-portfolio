-- ============================================
-- SETUP-скрипт для создания схем и таблиц
-- ============================================

-- 1. Создание схем
CREATE SCHEMA IF NOT EXISTS VT260224AD30FB__STAGING;
CREATE SCHEMA IF NOT EXISTS VT260224AD30FB__DWH;

-- 2. STG-слой: таблица transactions
DROP TABLE IF EXISTS VT260224AD30FB__STAGING.transactions;
CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.transactions 
(
    operation_id        VARCHAR(60),
    account_number_from INT,
    account_number_to   INT,
    currency_code       INT,
    country             VARCHAR(30),
    status              VARCHAR(30),
    transaction_type    VARCHAR(30),
    amount              INT,
    transaction_dt      TIMESTAMP
)
ORDER BY transaction_dt
SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES;

-- 3. STG-слой: таблица currencies
DROP TABLE IF EXISTS VT260224AD30FB__STAGING.currencies;
CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.currencies
(
    date_update        TIMESTAMP,
    currency_code      INT,
    currency_code_with INT,
    currency_with_div  NUMERIC(5,3)
)
ORDER BY date_update
SEGMENTED BY HASH(currency_code, date_update) ALL NODES;

-- 4. STG-слой: таблица логов load_log
DROP TABLE IF EXISTS VT260224AD30FB__STAGING.load_log;
CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.load_log
(
    schema_name   VARCHAR(100),
    table_name    VARCHAR(100),
    load_date     DATE,
    load_end      TIMESTAMP,
    rows_loaded   INTEGER,
    status        VARCHAR(20),
    error_message VARCHAR(1000)
)
ORDER BY load_end
SEGMENTED BY HASH(table_name, load_end) ALL NODES;

-- 5. DWH-слой: витрина global_metrics
DROP TABLE IF EXISTS VT260224AD30FB__DWH.global_metrics;
CREATE TABLE IF NOT EXISTS VT260224AD30FB__DWH.global_metrics
(
    date_update                    TIMESTAMP NOT NULL,
    currency_from                  INT NOT NULL,
    amount_total                   INT NOT NULL,
    cnt_transactions               INT NOT NULL,
    avg_transactions_per_account   NUMERIC(14,2) NOT NULL,
    cnt_accounts_make_transactions INT NOT NULL,
    CONSTRAINT pk_global_metrics PRIMARY KEY (date_update, currency_from)
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;

-- 6. DWH-слой: таблица логов load_log
DROP TABLE IF EXISTS VT260224AD30FB__DWH.load_log;
CREATE TABLE IF NOT EXISTS VT260224AD30FB__DWH.load_log
(
    schema_name    VARCHAR(100),
    table_name     VARCHAR(100),
    load_date      TIMESTAMP,
    load_end       TIMESTAMP,
    status         VARCHAR(20),
    error_message  VARCHAR(1000)
)
ORDER BY load_end
SEGMENTED BY HASH(table_name, load_end) ALL NODES;