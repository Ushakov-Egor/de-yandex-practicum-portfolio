-- DDL для mart.f_customer_retention с целью исследования возвращаемости клиентов

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