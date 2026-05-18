-- МИГРАЦИЯ ДАННЫХ ДЛЯ mart.f_sales

-- Создание временной таблицы mart.temp_f_sales
drop table if exists mart.temp_f_sales cascade;
CREATE TABLE if not exists mart.temp_f_sales (
	id serial4 NOT NULL,
	date_id int4 NOT NULL,
	item_id int4 NOT NULL,
	customer_id int4 NOT NULL,
	city_id int4 NOT NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	status varchar(8) not null,
	CONSTRAINT f_sales_pkey_1 PRIMARY KEY (id),
	CONSTRAINT f_sales_customer_id_fkey_1 FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id),
	CONSTRAINT f_sales_date_id_fkey_1 FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id),
	CONSTRAINT f_sales_item_id_fkey_1 FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id),
	CONSTRAINT f_sales_item_id_fkey1_1 FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id)
);
CREATE INDEX f_ds1_1 ON mart.temp_f_sales USING btree (date_id);
CREATE INDEX f_ds2_1 ON mart.temp_f_sales USING btree (item_id);
CREATE INDEX f_ds3_1 ON mart.temp_f_sales USING btree (customer_id);
CREATE INDEX f_ds4_1 ON mart.temp_f_sales USING btree (city_id);

-- Миграция данных из mart.f_sales в mart.temp_f_sales
insert into mart.temp_f_sales (id, date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select id, date_id, item_id, customer_id, city_id, quantity, payment_amount, 'shipped'
from mart.f_sales;

-- Удаление старой таблицы mart.f_sales
drop table if exists mart.f_sales cascade;

-- Переименование новой таблицы mart.temp_f_sales в mart.f_sales
alter table mart.temp_f_sales rename to f_sales;