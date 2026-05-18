MERGE INTO {table_name} AS target
USING (
    SELECT 
        almost_final.date_update,
        currency_from,
        CASE
            WHEN currency_from = 420 THEN almost_final.amount_total
            WHEN currency_from <> 420 THEN amount_total * usd_course.currency_with_div
        END AS amount_total,
        cnt_transactions,
        avg_transactions_per_account,
        cnt_accounts_make_transactions
    FROM (
        SELECT
            CAST(stg_tr.transaction_dt AS DATE) AS date_update,
            stg_tr.currency_code AS currency_from,
            SUM(stg_tr.amount) AS amount_total,
            COUNT(*) AS cnt_transactions,
            CAST((SUM(stg_tr.amount) / COUNT(DISTINCT(stg_tr.account_number_from))) AS NUMERIC(14,2)) AS avg_transactions_per_account,
            COUNT(DISTINCT(stg_tr.account_number_from)) AS cnt_accounts_make_transactions
        FROM (
            SELECT *
            FROM {stg_transactions}
            WHERE account_number_from >= 0
            	AND status = 'done'
                AND CAST(transaction_dt AS DATE) = CAST(:load_date AS DATE)
        ) AS stg_tr
        GROUP BY date_update, currency_from
    ) AS almost_final
    LEFT JOIN (
        SELECT date_update,currency_code, currency_with_div
        FROM {stg_currencies}
        WHERE CAST(date_update AS DATE) = CAST(:load_date AS DATE)
            AND currency_code_with = 420
    ) AS usd_course ON almost_final.currency_from = usd_course.currency_code AND almost_final.date_update = usd_course.date_update
) AS source ON target.date_update = source.date_update AND target.currency_from = source.currency_from
WHEN MATCHED THEN UPDATE SET
    amount_total = source.amount_total,
    cnt_transactions = source.cnt_transactions,
    avg_transactions_per_account = source.avg_transactions_per_account,
    cnt_accounts_make_transactions = source.cnt_accounts_make_transactions
WHEN NOT MATCHED THEN INSERT 
    (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
VALUES 
    (source.date_update, source.currency_from, source.amount_total, source.cnt_transactions, source.avg_transactions_per_account, source.cnt_accounts_make_transactions);