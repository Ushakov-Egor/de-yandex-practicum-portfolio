--truncate table dds.dm_deliveries restart identity cascade;
--select * from dds.dm_deliveries;

insert into dds.dm_deliveries (delivery_id, delivery_ts, order_id, order_ts, address_id, courier_id)
select
	stg_ds.object_id as delivery_id,
	(stg_ds.object_value::json ->> 'delivery_ts')::timestamp as delivery_ts,
	dm_o.id as order_id,
	(stg_ds.object_value::json ->> 'order_ts')::timestamp as order_ts,
	dm_adress.id as address_id,
	dm_cour.id as courier_id
from stg.deliverysystem_deliveries as stg_ds
join dds.dm_orders as dm_o on (stg_ds.object_value::json ->> 'order_id')::varchar = dm_o.order_key
join dds.dm_delivery_addresses as dm_adress on (stg_ds.object_value::json ->> 'address')::varchar = dm_adress.delivery_address
join dds.dm_couriers as dm_cour on (stg_ds.object_value::json ->> 'courier_id')::varchar = dm_cour.object_id
on conflict (delivery_id)
do update set
	delivery_ts = excluded.delivery_ts, 
	order_id = excluded.order_id, 
	order_ts = excluded.order_ts, 
	address_id = excluded.address_id, 
	courier_id = excluded.courier_id;