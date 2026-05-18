-- DDL таблицы load_log для записи логов о загрузке данных в стейнджинг-слой из системы источника

-- drop table if exists VT260224AD30FB__STAGING.load_log;

create table if not exists VT260224AD30FB__STAGING.load_log
(
	schema_name varchar(100),
	table_name varchar(100),
	load_date date,
	load_end timestamp,
	rows_loaded integer,
	status varchar(20),  -- 'SUCCESS', 'ERROR'
	error_message varchar(1000)
)
order by load_end
segmented by hash(table_name, load_end) all nodes;