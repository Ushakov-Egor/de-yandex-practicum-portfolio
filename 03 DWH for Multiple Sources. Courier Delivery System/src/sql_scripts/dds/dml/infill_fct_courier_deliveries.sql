--truncate table dds.fct_courier_deliveries restart identity cascade;  
--select * from dds.fct_courier_deliveries;

insert into dds.fct_courier_deliveries (courier_id, delivery_id, rate, delivery_sum, tip_sum)
select 
	dm_del.courier_id as courier_id,
	dm_del.id as delivery_id,
	(stg_del.object_value::json ->> 'rate')::int as rate,
	fct_p.total_sum as delivery_sum,
	(stg_del.object_value::json ->> 'tip_sum')::decimal(14,2) as tip_sum
from dds.dm_deliveries as dm_del
join stg.deliverysystem_deliveries stg_del on (stg_del.object_value::json ->> 'delivery_id')::varchar = dm_del.delivery_id
join dds.dm_orders dm_o on dm_del.order_id = dm_o.id
join (
	select  
		order_id,
		sum(total_sum) as total_sum
	from dds.fct_product_sales
	group by order_id) fct_p on dm_o.id = fct_p.order_id
on conflict(delivery_id) do update set
	courier_id = excluded.courier_id, 
	delivery_id = excluded.delivery_id, 
	rate = excluded.rate,
	delivery_sum = excluded.delivery_sum,
	tip_sum = excluded.tip_sum;