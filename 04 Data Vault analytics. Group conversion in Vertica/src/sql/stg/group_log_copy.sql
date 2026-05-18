-- Загрузка данных в VT260224AD30FB__STAGING.group_log с помощью COPY

COPY VT260224AD30FB__STAGING.group_log (group_id, user_id, user_id_from, event, datetime)
FROM LOCAL 'C:\WorkSpace\Learning\Data-Engineer\Sprint-6\s6-lessons\data\group_log.csv' -- локальный путь; при работе в Docker заменить на /src/data/group_log.csv
DELIMITER ','
SKIP 1
NULL AS ''
REJECTMAX 100
REJECTED DATA AS TABLE VT260224AD30FB__STAGING.group_log_rej
;
