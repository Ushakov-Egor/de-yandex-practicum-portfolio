-- Заполнение сателлита s_auth_history данными из STG слоя.
-- JOIN через хабы h_groups и h_users позволяет получить хэш-ключ линка luga.hk_l_user_group_activity.
-- Условие WHERE исключает уже загруженные записи (идемпотентность).

INSERT INTO VT260224AD30FB__DWH.s_auth_history (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT
    luga.hk_l_user_group_activity,
    sgl.user_id_from,
    sgl.event,
    sgl.datetime AS event_dt,
    now()        AS load_dt,
    's3'         AS load_src
FROM VT260224AD30FB__STAGING.group_log AS sgl
LEFT JOIN VT260224AD30FB__DWH.h_groups               hg   ON sgl.group_id  = hg.group_id
LEFT JOIN VT260224AD30FB__DWH.h_users                hu   ON sgl.user_id   = hu.user_id
LEFT JOIN VT260224AD30FB__DWH.l_user_group_activity  luga ON hg.hk_group_id = luga.hk_group_id
                                                          AND hu.hk_user_id  = luga.hk_user_id
WHERE luga.hk_l_user_group_activity NOT IN (
    SELECT hk_l_user_group_activity FROM VT260224AD30FB__DWH.s_auth_history
)
;
