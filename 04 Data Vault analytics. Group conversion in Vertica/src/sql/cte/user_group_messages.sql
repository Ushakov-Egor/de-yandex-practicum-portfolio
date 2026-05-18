-- CTE 7.1: user_group_messages
-- Подсчёт уникальных пользователей в каждой группе, которые написали хотя бы одно сообщение.
-- Источник: линк l_groups_dialogs связывает группы с диалогами,
--           линк l_user_message связывает сообщения с пользователями.

WITH user_group_messages AS (
    SELECT
        lgd.hk_group_id,
        count(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM VT260224AD30FB__DWH.l_groups_dialogs lgd
    JOIN VT260224AD30FB__DWH.h_dialogs        hd  ON hd.hk_message_id  = lgd.hk_message_id
    JOIN VT260224AD30FB__DWH.l_user_message   lum ON lum.hk_message_id = hd.hk_message_id
    GROUP BY lgd.hk_group_id
)

SELECT
    hk_group_id,
    cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages
LIMIT 10
;
