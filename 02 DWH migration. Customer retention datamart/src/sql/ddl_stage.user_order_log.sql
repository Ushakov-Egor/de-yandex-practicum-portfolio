-- staging.user_order_log определение

DROP TABLE staging.user_order_log;

CREATE TABLE staging.user_order_log (
	uniq_id varchar(32) NOT NULL,
	date_time timestamp NOT NULL,
	city_id int4 NOT NULL,
	city_name varchar(100) NULL,
	customer_id int4 NOT NULL,
	first_name varchar(100) NULL,
	last_name varchar(100) NULL,
	item_id int4 NOT NULL,
	item_name varchar(100) NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	status varchar(8) NULL,
	CONSTRAINT user_order_log_pk PRIMARY KEY (uniq_id)
);
CREATE INDEX uo1 ON staging.user_order_log USING btree (customer_id);
CREATE INDEX uo2 ON staging.user_order_log USING btree (item_id);