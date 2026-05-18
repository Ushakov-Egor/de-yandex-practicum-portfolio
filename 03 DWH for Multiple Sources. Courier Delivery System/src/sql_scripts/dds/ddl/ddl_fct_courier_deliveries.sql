drop table if exists dds.fct_courier_deliveries;

create table if not exists  dds.fct_courier_deliveries (
    id serial primary key,
    courier_id int4 not null,
    delivery_id int4 not null,
    rate int2 not null,
    delivery_sum numeric(14, 2) not null,
    tip_sum numeric(14, 2) not null,
    -- constraint fk_delivery_details_courier foreign key (courier_id) references dds.dm_couriers(id),
    constraint fk_delivery_details_delivery foreign key (delivery_id) references dds.dm_deliveries(id),
    constraint uniq_delivery_details_delivery unique (delivery_id)
);