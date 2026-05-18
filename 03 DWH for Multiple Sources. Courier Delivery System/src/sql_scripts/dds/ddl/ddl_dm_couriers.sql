-- справочник курьеров
drop table if exists dds.dm_couriers;

create table if not exists dds.dm_couriers (
    id serial primary key,              -- суррогатный ключ
    object_id varchar not null unique,  -- идентификатор курьера из источника (_id)
    name varchar not null               -- имя курьера
);