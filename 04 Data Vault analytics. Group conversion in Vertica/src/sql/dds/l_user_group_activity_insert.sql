-- Заполнение линка l_user_group_activity данными из STG слоя
-- Хэш-ключ линка вычисляется как hash(group_id, user_id).
-- Условие WHERE исключает уже загруженные записи (идемпотентность).

INSERT INTO VT260224AD30FB__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
SELECT DISTINCT
    hash(hg.group_id, hu.user_id) AS hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    now()                         AS load_dt,
    's3'                          AS load_src
FROM VT260224AD30FB__STAGING.group_log AS sgl
LEFT JOIN VT260224AD30FB__DWH.h_users  hu ON sgl.user_id  = hu.user_id
LEFT JOIN VT260224AD30FB__DWH.h_groups hg ON sgl.group_id = hg.group_id
WHERE hash(hg.group_id, hu.user_id) NOT IN (
    SELECT hk_l_user_group_activity FROM VT260224AD30FB__DWH.l_user_group_activity
)
;
