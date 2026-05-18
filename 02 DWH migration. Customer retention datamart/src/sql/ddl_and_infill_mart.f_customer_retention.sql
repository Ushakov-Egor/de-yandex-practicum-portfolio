-- DDL и заполнение mart.f_customer_retention

drop table if exists mart.f_customer_retention cascade;
create table if not exists mart.f_customer_retention (
    id serial primary key,
    period_name varchar(10) not null default 'weekly',
    period_id integer not null,
    item_id integer not null,
    new_customers_count integer default 0,
    returning_customers_count integer default 0,
    refunded_customer_count integer default 0,
    new_customers_revenue numeric(10,2) default 0,
    returning_customers_revenue numeric(10,2) default 0,
    customers_refunded integer default 0,
    constraint f_customer_retention_item_fkey foreign key (item_id) 
        references mart.d_item(item_id)
);

create index idx_fcr_period_item on mart.f_customer_retention (period_id, item_id);
create index idx_fcr_item on mart.f_customer_retention (item_id);

truncate table mart.f_customer_retention cascade;

insert into mart.f_customer_retention (period_id, item_id, new_customers_count, returning_customers_count, refunded_customer_count, new_customers_revenue,returning_customers_revenue,customers_refunded)
select
    to_char(t1.date_actual, 'YYYYWW')::integer as period_id,
    t2.item_id as item_id,
    count(distinct t3.customer_id) filter (where order_count <= 1) as new_customers_count,           -- кол-во новых клиентов (тех, которые сделали только один заказ за рассматриваемый промежуток времени)
    count(distinct t3.customer_id) filter (where order_count > 1) as returning_customers_count,      -- кол-во вернувшихся клиентов (тех, которые сделали только несколько заказов за рассматриваемый промежуток времени)
    count(t3.refunded_customer_count) as refunded_customer_count,                                    -- кол-во клиентов, оформивших возврат за рассматриваемый промежуток времени
    sum(t3.sum_revenue) filter (where order_count <= 1) as new_customers_revenue,                    -- доход с новых клиентов
    sum(t3.sum_revenue) filter (where order_count > 1) as returning_customers_revenue,               -- доход с вернувшихся клиентов
	sum(t3.refunded_customer_count) as customers_refunded                                            -- количество возвратов клиентов
from mart.d_calendar t1
join mart.f_sales t2 on t1.date_id = t2.date_id
join (
	-- подзапрос для расчета показателей за неделю для каждого клиента
	select
	    customer_id,
	    item_id,
	    to_char(dc2.date_actual, 'YYYYWW') as week,
	    count(*) as order_count,                                                                     -- расчет, сколько заказов сделал клиент в течении отчетного периода в каждой категории товара (за неделю)
	    count(*) filter (where fs2.status = 'refunded') as refunded_customer_count,                  -- расчет, сколько возвратов сделал клиент в отчетном периодне в каждой категории товара (за неделю)
		sum(fs2.payment_amount) as sum_revenue                                                       -- суммарный доход с 1 клиента в каждой категории товара
	from mart.f_sales fs2
	join mart.d_calendar dc2 on fs2.date_id = dc2.date_id
	group by customer_id, item_id, to_char(dc2.date_actual, 'YYYYWW')
	) t3 on t2.customer_id = t3.customer_id and t2.item_id = t3.item_id and to_char(t1.date_actual, 'YYYYWW') = t3.week
group by period_id, t2.item_id
order by period_id, t2.item_id;


