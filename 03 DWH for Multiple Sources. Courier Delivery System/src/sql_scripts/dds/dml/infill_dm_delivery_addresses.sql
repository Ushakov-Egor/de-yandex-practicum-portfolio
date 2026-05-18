--truncate table dds.dm_delivery_addresses restart identity cascade;
--select * from dds.dm_delivery_addresses;

insert into dds.dm_delivery_addresses (delivery_address)
select
	distinct((object_value::json ->> 'address')::varchar) as delivery_address
from stg.deliverysystem_deliveries
on conflict (delivery_address) do nothing;