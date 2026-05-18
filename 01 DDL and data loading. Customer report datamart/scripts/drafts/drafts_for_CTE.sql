SELECT * 
FROM (
	SELECT 
		T1.customer_id AS customer_id,
		COUNT(T1.customer_id) AS count
	FROM(
		select 
			fo.customer_id AS customer_id,
			dcust.customer_name AS customer_name,
			dcust.customer_address AS customer_address,
			dcust.customer_birthday AS customer_birthday,
			dcust.customer_email AS customer_email,
			dprod.load_dttm AS customer_load_dttm,
			dcrm.customer_id AS exist_customer_id
		from dwh.f_order fo 
			JOIN dwh.d_craftsman dcraft ON fo.craftsman_id = dcraft.craftsman_id
			JOIN dwh.d_customer dcust ON fo.customer_id = dcust.customer_id
			JOIN dwh.d_product dprod ON fo.product_id = dprod.product_id
			LEFT JOIN dwh.customer_report_datamart dcrm ON fo.customer_id = dcrm.customer_id
		ORDER BY fo.customer_id, dcust.customer_name
	) AS T1
	GROUP BY customer_id
) AS T2
WHERE count > 1;


CREATE TEMP TABLE temp AS (
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
		dcraft.craftsman_id AS craftsman_id
	FROM dwh.f_order fo 
		JOIN dwh.d_craftsman dcraft ON fo.craftsman_id = dcraft.craftsman_id
		JOIN dwh.d_customer dcust ON fo.customer_id = dcust.customer_id
		JOIN dwh.d_product dprod ON fo.product_id = dprod.product_id
		LEFT JOIN dwh.customer_report_datamart dcrm ON fo.customer_id = dcrm.customer_id
	ORDER BY fo.customer_id, dcust.customer_name
);

SELECT customer_id, COUNT(craftsman_id)
FROM temp
GROUP BY customer_id
ORDER BY COUNT(craftsman_id) DESC;


SELECT 
	T3.customer_id,
	T3.craftsman_id,
	DENSE_RANK() OVER (ORDER BY T3.craftsman_count DESC) AS rank_count_craftsman
FROM (
	SELECT 
		dd.customer_id,
		dd.craftsman_id,
		COUNT(dd.craftsman_id) AS craftsman_count
	FROM dwh_delta AS dd
	GROUP BY dd.customer_id, dd.craftsman_id
	ORDER BY craftsman_count DESC
) AS T3;



/* Делаем расчёт витрины по новым данным. 
 * Этой информации по заказчикам в рамках расчётного периода раньше не было, это новые данные. 
 * Их можно просто вставить (insert) в витрину без обновления */

SELECT 
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
    SELECT 
        T1.customer_id AS customer_id,
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
        
    FROM dwh_delta AS T1
    WHERE T1.exist_customer_id IS NULL
    GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
) AS T2
JOIN (
    SELECT 
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
    SELECT 
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

) AS T7 ORDER BY report_period;



/* Делаем перерасчёт для существующих записей витрины, так как данные обновились за отчётные периоды. 
 * Логика похожа на insert, но нужно достать конкретные данные из DWH */

DROP TABLE IF EXISTS dwh_update_delta CASCADE;
CREATE TABLE IF NOT EXISTS dwh_update_delta AS (
	SELECT 
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
		SELECT
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
		    SELECT 
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
		    SELECT 
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
);
















