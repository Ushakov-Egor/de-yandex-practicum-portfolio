-- DDL для таблицы доставок из источника данных по доставкам
drop table if exists stg.deliverysystem_deliveries;
create table if not exists stg.deliverysystem_deliveries (
	id serial primary key,
	object_id varchar,
	object_value json
);