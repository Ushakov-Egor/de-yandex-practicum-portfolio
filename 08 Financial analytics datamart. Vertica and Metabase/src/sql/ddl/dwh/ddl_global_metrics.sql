-- DDL таблицы для витрины global_metrics в слое DWH

/*
	- date_update — дата расчёта,
    - currency_from — код валюты транзакции;
    - amount_total — общая сумма транзакций по валюте в долларах;
    - cnt_transactions — общий объём транзакций по валюте;
    - avg_transactions_per_account — средний объём транзакций с аккаунта;
    - cnt_accounts_make_transactions — количество уникальных аккаунтов с совершёнными транзакциями по валюте.
*/

-- drop table if exists VT260224AD30FB__DWH.global_metrics;

create table if not exists VT260224AD30FB__DWH.global_metrics
(
    date_update timestamp not null,
    currency_from int not null,
    amount_total int not null,
    cnt_transactions int not null,
    avg_transactions_per_account numeric(14,2) not null,
    cnt_accounts_make_transactions int not null,
    
    constraint pk_global_metrics primary key (date_update, currency_from)
)    
order by date_update, currency_from
segmented by hash(date_update, currency_from) all nodes
;