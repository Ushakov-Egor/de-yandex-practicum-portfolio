-- справочник уникальных адресов доставки
drop table if exists dds.dm_delivery_addresses;

create table if not exists dds.dm_delivery_addresses (
    id serial primary key,              -- суррогатный ключ
    delivery_address varchar not null unique   -- уникальный адрес доставки
);