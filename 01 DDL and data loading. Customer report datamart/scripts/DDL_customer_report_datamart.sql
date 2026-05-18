DROP TABLE IF EXISTS dwh.customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
	id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи
	customer_id BIGINT NOT NULL, -- идентификатор заказчика
	customer_name VARCHAR NOT NULL, -- Ф. И. О. заказчика
	customer_address VARCHAR NOT NULL, -- адрес заказчика
	customer_birthday DATE NOT NULL, -- дата рождения заказчика
	customer_email VARCHAR NOT NULL, -- электронная почта заказчика
	customer_costs INT NOT NULL, -- сумма, которую потратил заказчик
	platform_money INT NOT NULL, -- сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик)
	total_order_count INT, -- количество заказов у заказчика за месяц
	avg_order_price DECIMAL(10,2) NOT NULL, -- средняя стоимость одного заказа у заказчика за месяц
	median_time_order_completed NUMERIC(10,1), -- медианное время в днях от момента создания заказа до его завершения за месяц
	top_product_category VARCHAR NOT NULL, -- самая популярная категория товаров у этого заказчика за месяц
	top_craftsman_id BIGINT NOT NULL, -- идентификатор самого популярного мастера ручной работы у заказчика. Если заказчик сделал одинаковое количество заказов у нескольких мастеров, возьмите любого
	count_order_created INT NOT NULL, -- количество созданных заказов за месяц
	count_order_in_progress BIGINT NOT NULL, -- количество заказов в процессе изготовки за месяц
    count_order_delivery BIGINT NOT NULL, -- количество заказов в доставке за месяц
    count_order_done BIGINT NOT NULL, -- количество завершённых заказов за месяц
    count_order_not_done BIGINT NOT NULL, -- количество незавершённых заказов за месяц
    report_period VARCHAR NOT NULL, -- отчётный период (год и месяц)
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY(id)
);