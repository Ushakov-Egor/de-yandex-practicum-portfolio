--truncate table dds.dm_couriers restart identity cascade;
--select * from dds.dm_couriers;

insert into dds.dm_couriers (object_id, name)
select 
	object_id as object_id,
	(object_value::json ->> 'name')::varchar as name 
from stg.deliverysystem_couriers
on conflict (object_id)
do update set
	name = excluded.name;