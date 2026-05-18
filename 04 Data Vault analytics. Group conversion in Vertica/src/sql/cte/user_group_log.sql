-- CTE 7.2: user_group_log
-- Подсчёт уникальных пользователей, которые вступили в группу (event = 'add').
-- Источник: сателлит s_auth_history хранит все события пользователей в группах,
--           линк l_user_group_activity связывает пользователей с группами.

WITH user_group_log AS (
    SELECT
        lga.hk_group_id,
        count(DISTINCT sah.user_id_from) AS cnt_added_users
    FROM VT260224AD30FB__DWH.s_auth_history        sah
    JOIN VT260224AD30FB__DWH.l_user_group_activity lga ON lga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    WHERE sah.event = 'add'
    GROUP BY lga.hk_group_id
)

SELECT
    hk_group_id,
    cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users
LIMIT 10
;
