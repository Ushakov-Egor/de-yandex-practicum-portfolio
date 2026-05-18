-- select * from cdm.dm_courier_report;

truncate table cdm.dm_courier_report restart identity cascade;
insert into cdm.dm_courier_report (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
select
	fct_c.courier_id as courier_id,
	dm_c.name as courier_name,
	extract(year from dm_del.delivery_ts) as settlement_year,
	extract(month from dm_del.delivery_ts) as settlement_month,
	count(dm_del.order_id) as orders_count,
	sum(fct_c.delivery_sum) as orders_total_sum,
	avg(fct_c.rate) as rate_avg,
	sum(fct_c.delivery_sum) * 0.25 as order_processing_fee,
	sum(fct_c.delivery_sum) *
		case 
			when avg(fct_c.rate) < 4 then 0.05
			when avg(fct_c.rate) < 4.5 then 0.07
			when avg(fct_c.rate) < 4.9 then 0.08
			else 0.10
		end as courier_order_sum,
	sum(fct_c.tip_sum) as courier_tips_sum,
	(sum(fct_c.delivery_sum) *
		case 
			when avg(fct_c.rate) < 4 then 0.05
			when avg(fct_c.rate) < 4.5 then 0.07
			when avg(fct_c.rate) < 4.9 then 0.08
			else 0.10
		end + sum(fct_c.tip_sum)) * 0.95 as courier_reward_sum
from dds.fct_courier_deliveries as fct_c
join dds.dm_couriers as dm_c on fct_c.courier_id = dm_c.id
join dds.dm_deliveries as dm_del on fct_c.delivery_id = dm_del.id
group by fct_c.courier_id, dm_c.name, extract(year from dm_del.delivery_ts), extract(month from dm_del.delivery_ts)
order by dm_c.name
;