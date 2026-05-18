-- DDL для таблицы курьеров из источника данных по доставкам
drop table if exists stg.deliverysystem_couriers;
create table if not exists stg.deliverysystem_couriers (
	id serial primary key,
	object_id varchar,
	object_value json
);