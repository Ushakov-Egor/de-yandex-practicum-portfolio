--DDL таблицы currencies для загрузки в нее данных из системы источника

-- drop table if exists VT260224AD30FB__STAGING.currencies;

create table if not exists VT260224AD30FB__STAGING.currencies
(
	date_update timestamp NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div numeric(5, 3) NULL
)
order by date_update
segmented by hash(currency_code, date_update) all nodes
;