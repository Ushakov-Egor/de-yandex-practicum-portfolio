-- DDL для витрины курьеров
DROP TABLE IF EXISTS cdm.dm_courier_report;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_report (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year INTEGER NOT NULL,
    settlement_month INTEGER NOT NULL,
    orders_count INTEGER NOT NULL DEFAULT 0,
    orders_total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    rate_avg NUMERIC(3, 2) NOT NULL DEFAULT 0,
    order_processing_fee NUMERIC(14, 2) NOT NULL DEFAULT 0,
    courier_order_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    courier_tips_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    courier_reward_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    
    -- Проверки для месяцев
    CONSTRAINT dm_courier_report_settlement_month_check 
        CHECK (settlement_month BETWEEN 1 AND 12),
    
    -- Проверка для года (разумные пределы)
    CONSTRAINT dm_courier_report_settlement_year_check 
        CHECK (settlement_year BETWEEN 2020 AND 2100),
    
    -- Проверки для числовых полей (неотрицательные)
    CONSTRAINT dm_courier_report_orders_count_check 
        CHECK (orders_count >= 0),
    CONSTRAINT dm_courier_report_orders_total_sum_check 
        CHECK (orders_total_sum >= 0),
    CONSTRAINT dm_courier_report_rate_avg_check 
        CHECK (rate_avg BETWEEN 1 AND 5),
    CONSTRAINT dm_courier_report_order_processing_fee_check 
        CHECK (order_processing_fee >= 0),
    CONSTRAINT dm_courier_report_courier_order_sum_check 
        CHECK (courier_order_sum >= 0),
    CONSTRAINT dm_courier_report_courier_tips_sum_check 
        CHECK (courier_tips_sum >= 0),
    CONSTRAINT dm_courier_report_courier_reward_sum_check 
        CHECK (courier_reward_sum >= 0)
);