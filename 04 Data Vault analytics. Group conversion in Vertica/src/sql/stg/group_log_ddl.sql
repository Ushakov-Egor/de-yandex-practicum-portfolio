-- DDL таблицы для данных о логе групп в STG слое

DROP TABLE IF EXISTS VT260224AD30FB__STAGING.group_log;

CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.group_log
(
    group_id     INT         NOT NULL PRIMARY KEY,
    user_id      INT         NOT NULL,
    user_id_from INT,
    event        VARCHAR(6),
    datetime     DATETIME
)
ORDER BY group_id
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2)
;
