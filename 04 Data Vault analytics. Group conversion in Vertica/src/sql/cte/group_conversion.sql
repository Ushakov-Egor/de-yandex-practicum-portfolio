-- CTE 7.3: group_conversion
-- Итоговый запрос для ответа на бизнес-вопрос:
-- для 10 самых старых групп рассчитать конверсию участников в первое сообщение.
--
-- oldest_groups   — 10 групп с наименьшим registration_dt из хаба h_groups
-- user_group_log  — количество уникальных пользователей, вступивших в группу (event = 'add')
-- user_group_messages — количество уникальных пользователей, написавших хотя бы одно сообщение
--
-- group_conversion = cnt_users_in_group_with_messages / cnt_added_users

WITH user_group_log AS (
    SELECT
        lga.hk_group_id,
        count(DISTINCT sah.user_id_from) AS cnt_added_users
    FROM VT260224AD30FB__DWH.s_auth_history        sah
    JOIN VT260224AD30FB__DWH.l_user_group_activity lga ON lga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    WHERE sah.event = 'add'
    GROUP BY lga.hk_group_id
)
, user_group_messages AS (
    SELECT
        lgd.hk_group_id,
        count(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM VT260224AD30FB__DWH.l_groups_dialogs lgd
    JOIN VT260224AD30FB__DWH.h_dialogs        hd  ON hd.hk_message_id  = lgd.hk_message_id
    JOIN VT260224AD30FB__DWH.l_user_message   lum ON lum.hk_message_id = hd.hk_message_id
    GROUP BY lgd.hk_group_id
)
, oldest_groups AS (
    SELECT hk_group_id
    FROM VT260224AD30FB__DWH.h_groups
    ORDER BY registration_dt ASC
    LIMIT 10
)

SELECT
    og.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users::FLOAT AS group_conversion
FROM oldest_groups og
LEFT JOIN user_group_log      ugl ON og.hk_group_id = ugl.hk_group_id
LEFT JOIN user_group_messages ugm ON og.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC
;
