--DDL таблицы transactions для загрузки в нее данных из системы источника

-- drop table if exists VT260224AD30FB__STAGING.transactions;

create table if not exists VT260224AD30FB__STAGING.transactions 
(
	operation_id varchar(60) NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt timestamp null
)
order by transaction_dt
segmented by HASH(operation_id, transaction_dt) all nodes
;