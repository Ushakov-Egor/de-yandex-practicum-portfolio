/* ИНКРЕМЕНТАЛЬНОЕ ОБНОВЛЕНИЕ ВИТРИНЫ customer_report_datamart */

WITH

/* определяем, какие данные были изменены в витрине или добавлены в DWH, формируем дельту изменений */
dwh_delta AS (
	SELECT
		fo.customer_id AS customer_id,
		dcrm.customer_id AS exist_customer_id,
		dcust.customer_name AS customer_name,
		dcust.customer_address AS customer_address,
		dcust.customer_birthday AS customer_birthday,
		dcust.customer_email AS customer_email,
		dprod.product_price AS product_price,
		dprod.product_type AS product_type,
		dprod.load_dttm AS customer_load_dttm,
		dcraft.craftsman_id AS craftsman_id,
		fo.order_id AS order_id,
		fo.order_completion_date - fo.order_created_date AS diff_order_date,
		fo.order_status AS order_status,
		TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
		dcraft.load_dttm AS craftsman_load_dttm,
		dcust.load_dttm AS customers_load_dttm,
		dprod.load_dttm AS products_load_dttm
		
	FROM dwh.f_order fo
	JOIN dwh.d_craftsman dcraft ON fo.craftsman_id = dcraft.craftsman_id
	JOIN dwh.d_customer dcust ON fo.customer_id = dcust.customer_id
	JOIN dwh.d_product dprod ON fo.product_id = dprod.product_id
	LEFT JOIN dwh.customer_report_datamart dcrm ON fo.customer_id = dcrm.customer_id
		WHERE 
			(fo.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
			(dcraft.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
			(dcust.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
			(dprod.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),

/* определяем заказчиков, по которым нужно обновить данные, т.к. они уже есть в витрине */
dwh_update_delta AS (
    SELECT customer_id AS customer_id
	FROM dwh_delta dd
	WHERE dd.exist_customer_id IS NOT NULL
),

/* Рассчитываем атрибуты для витрины и готовим выборку для вставки по НОВЫМ заказчикам */
dwh_delta_insert_result AS (
    SELECT -- итоговая выборка для НОВЫХ заказчиков, полностью соответсвующая витрине
		T7.customer_id AS customer_id,
		T7.customer_name AS customer_name,
		T7.customer_address AS customer_address,
	    T7.customer_birthday AS customer_birthday,
	    T7.customer_email AS customer_email,
	    T7.customer_costs AS customer_costs,
	    T7.platform_money AS platform_money,
	    T7.total_order_count AS total_order_count,
	    T7.avg_order_price AS avg_order_price,
	    T7.median_time_order_completed AS median_time_order_completed,
	    T7.top_craftsman_id AS top_craftsman_id,
	    T7.top_product_type AS top_product_category,
	    T7.count_order_created AS count_order_created,
	    T7.count_order_in_progress AS count_order_in_progress,
	    T7.count_order_delivery AS count_order_delivery,
	    T7.count_order_done AS count_order_done,
	    T7.count_order_not_done AS count_order_not_done,
	    T7.report_period AS report_period
		
	FROM ((
	    SELECT -- расчет по большинству столбцов для НОВЫХ заказчиков
	        T1.customer_id AS customer_id,
	        T1.customer_name AS customer_name,
	        T1.customer_address AS customer_address,
	        T1.customer_birthday AS customer_birthday,
	        T1.customer_email AS customer_email,
	        SUM(T1.product_price) AS customer_costs,
	        SUM(T1.product_price)*0.1 AS platform_money,
	        COUNT(T1.order_id) AS total_order_count,
	        AVG(T1.product_price) AS avg_order_price,
	        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY T1.diff_order_date) AS median_time_order_completed,
	        SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
	        SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
	        SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
	        SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
	        SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
	        T1.report_period AS report_period
	        
	    FROM dwh_delta AS T1
	    WHERE T1.exist_customer_id IS NULL
	    GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
		) AS T2
		JOIN ( 
		    SELECT -- здесь определяем САМОГО ПОПУЛЯРНОГО МАСТЕРА для заказчика
		        T3.customer_id AS customer_id_T4,
		        T3.craftsman_id AS top_craftsman_id,
		        DENSE_RANK() OVER (PARTITION BY T3.customer_id ORDER BY T3.craftsman_count DESC) AS rank_count_craftsman
		    FROM (
		        SELECT 
		            dd.customer_id,
		            dd.craftsman_id,
		            COUNT(dd.craftsman_id) AS craftsman_count
		        FROM dwh_delta AS dd
		        GROUP BY dd.customer_id, dd.craftsman_id
		    ) AS T3
		) AS T4 ON T2.customer_id = T4.customer_id_T4 AND T4.rank_count_craftsman = 1
		
		JOIN
		
		(
		    SELECT -- здесь определяем САМУЮ ПОПУЛЯРНУЮ КАТЕГОРИЮ ТОВАРА для заказчика
		        T5.customer_id AS customer_id_T6,
		        T5.product_type AS top_product_type,
		        DENSE_RANK() OVER (PARTITION BY T5.customer_id ORDER BY T5.product_type DESC) AS rank_count_product_type
		    FROM (
		        SELECT 
		            dd.customer_id,
		            dd.product_type,
		            COUNT(dd.product_type) AS product_type_count
		        FROM dwh_delta AS dd
		        GROUP BY dd.customer_id, dd.product_type
		    ) AS T5
		) AS T6 ON T2.customer_id = T6.customer_id_T6 AND T6.rank_count_product_type = 1
		
		) AS T7 ORDER BY report_period
),
		
/* Рассчитываем атрибуты для витрины и готовим выборку для вставки по УЖЕ ИМЕЮЩИМСЯ в витрине заказчикам */
dwh_delta_update_result AS ( 
    SELECT -- итоговая выборка для УЖЕ ИМЕЮЩИХСЯ в витрине заказчиков, полностью соответсвующая витрине
		T7.customer_id AS customer_id,
		T7.customer_name AS customer_name,
		T7.customer_address AS customer_address,
	    T7.customer_birthday AS customer_birthday,
	    T7.customer_email AS customer_email,
	    T7.customer_costs AS customer_costs,
	    T7.platform_money AS platform_money,
	    T7.total_order_count AS total_order_count,
	    T7.avg_order_price AS avg_order_price,
	    T7.median_time_order_completed AS median_time_order_completed,
	    T7.top_craftsman_id AS top_craftsman_id,
	    T7.top_product_type AS top_product_category,
	    T7.count_order_created AS count_order_created,
	    T7.count_order_in_progress AS count_order_in_progress,
	    T7.count_order_delivery AS count_order_delivery,
	    T7.count_order_done AS count_order_done,
	    T7.count_order_not_done AS count_order_not_done,
	    T7.report_period AS report_period
	
	FROM ((
		SELECT -- расчет по большинству столбцов для УЖЕ ИМЕЮЩИХСЯ в витрине заказчиков
			T1.customer_id AS customer_id,
			T1.customer_id AS exist_customer_id,
			T1.customer_name AS customer_name,
			T1.customer_address AS customer_address,
			T1.customer_birthday AS customer_birthday,
			T1.customer_email AS customer_email,
			SUM(T1.product_price) AS customer_costs,
		    SUM(T1.product_price)*0.1 AS platform_money,
		    COUNT(order_id) AS total_order_count,
		    AVG(T1.product_price) AS avg_order_price,
		    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY T1.diff_order_date) AS median_time_order_completed,
		    SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
		    SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
		    SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
		    SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
		    SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
		    T1.report_period AS report_period
			
		FROM dwh_delta T1
		WHERE T1.exist_customer_id IS NOT NULL
		GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
		) AS T2
		
		JOIN ( 
		    SELECT -- здесь определяем САМОГО ПОПУЛЯРНОГО МАСТЕРА для заказчика
		        T3.customer_id AS customer_id_T4,
		        T3.craftsman_id AS top_craftsman_id,
		        DENSE_RANK() OVER (PARTITION BY T3.customer_id ORDER BY T3.craftsman_count DESC) AS rank_count_craftsman
		    FROM (
		        SELECT 
		            dd.customer_id,
		            dd.craftsman_id,
		            COUNT(dd.craftsman_id) AS craftsman_count
		        FROM dwh_delta AS dd
		        GROUP BY dd.customer_id, dd.craftsman_id
		    ) AS T3
		) AS T4 ON T2.customer_id = T4.customer_id_T4 AND T4.rank_count_craftsman = 1
		
		JOIN (
		    SELECT -- здесь определяем САМУЮ ПОПУЛЯРНУЮ КАТЕГОРИЮ ТОВАРА для заказчика
		        T5.customer_id AS customer_id_T6,
		        T5.product_type AS top_product_type,
		        DENSE_RANK() OVER (PARTITION BY T5.customer_id ORDER BY T5.product_type DESC) AS rank_count_product_type
		    FROM (
		        SELECT 
		            dd.customer_id,
		            dd.product_type,
		            COUNT(dd.product_type) AS product_type_count
		        FROM dwh_delta AS dd
		        GROUP BY dd.customer_id, dd.product_type
		    ) AS T5
		) AS T6 ON T2.customer_id = T6.customer_id_T6 AND T6.rank_count_product_type = 1
	) AS T7 ORDER BY report_period
),

/* Вставка в витрину НОВЫХ данных */
insert_delta AS (
    INSERT INTO dwh.customer_report_datamart (
		customer_id,
		customer_name,
		customer_address,
		customer_birthday,
		customer_email,
		customer_costs,
		platform_money,
		total_order_count,
		avg_order_price,
		median_time_order_completed,
		top_product_category,
		top_craftsman_id,
		count_order_created,
		count_order_in_progress,
		count_order_delivery,
		count_order_done,
		count_order_not_done,
		report_period
	)
	SELECT 
		src.customer_id,
		src.customer_name,
		src.customer_address,
		src.customer_birthday,
		src.customer_email,
		src.customer_costs,
		src.platform_money,
		src.total_order_count,
		src.avg_order_price,
		src.median_time_order_completed,
		src.top_product_category,
		src.top_craftsman_id,
		src.count_order_created,
		src.count_order_in_progress,
		src.count_order_delivery,
		src.count_order_done,
		src.count_order_not_done,
		src.report_period
		
	FROM dwh_delta_insert_result as src
),

/* Обновление в витрине УЖЕ ИМЕЮЩИХСЯ данных */
update_delta AS (
    UPDATE dwh.customer_report_datamart AS T1
	SET 
		customer_id = T2.customer_id,
		customer_name = T2.customer_name,
		customer_address = T2.customer_address,
		customer_birthday = T2.customer_birthday,
		customer_email = T2.customer_email,
		customer_costs = T2.customer_costs,
		platform_money = T2.platform_money,
		total_order_count = T2.total_order_count,
		avg_order_price = T2.avg_order_price,
		median_time_order_completed = T2.median_time_order_completed,
		top_product_category = T2.top_product_category,
		top_craftsman_id = T2.top_craftsman_id,
		count_order_created = T2.count_order_created,
		count_order_in_progress = T2.count_order_in_progress,
		count_order_delivery = T2.count_order_delivery,
		count_order_done = T2.count_order_done,
		count_order_not_done = T2.count_order_not_done,
		report_period = T2.report_period
	
	FROM dwh_delta_update_result AS T2
	WHERE T1.customer_id = T2.customer_id
),

/* Запись врмени последнего обновления в таблицу load_dates_customer_report_datamart */
insert_load_date AS (
    INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
	SELECT 
		GREATEST(
			COALESCE(MAX(craftsman_load_dttm), NOW()), 
		    COALESCE(MAX(customers_load_dttm), NOW()), 
		    COALESCE(MAX(products_load_dttm), NOW())
		) 
	FROM public.dwh_delta
)

SELECT 'increment datamart';