-- 6. DWH-слой: таблица логов load_log
-- DROP TABLE IF EXISTS VT260224AD30FB__DWH.load_log;
CREATE TABLE IF NOT EXISTS VT260224AD30FB__DWH.load_log
(
    schema_name    VARCHAR(100),
    table_name     VARCHAR(100),
    load_date      TIMESTAMP,
    load_end       TIMESTAMP,
    status         VARCHAR(20),
    error_message  VARCHAR(1000)
)
ORDER BY load_end
SEGMENTED BY HASH(table_name, load_end) ALL NODES;