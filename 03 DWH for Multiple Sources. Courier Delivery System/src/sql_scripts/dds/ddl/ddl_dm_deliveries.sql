
-- dds.dm_deliveries определение

-- Drop table

-- DROP TABLE dds.dm_deliveries;

CREATE TABLE dds.dm_deliveries (
	id serial4 NOT NULL,
	order_id int4 NOT NULL,
	address_id int4 NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int2 NOT NULL,
	sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_rate_check CHECK (((rate >= 1) AND (rate <= 5))),
	CONSTRAINT dm_deliveries_sum_check CHECK ((sum >= (0)::numeric)),
	CONSTRAINT dm_deliveries_tip_sum_check CHECK ((tip_sum >= (0)::numeric)),
	CONSTRAINT fk_deliveries_address FOREIGN KEY (address_id) REFERENCES dds.dm_delivery_addresses(id),
	CONSTRAINT fk_deliveries_order FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id)
);