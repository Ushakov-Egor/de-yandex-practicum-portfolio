-- DDL линка l_user_group_activity в слое DDS

DROP TABLE IF EXISTS VT260224AD30FB__DWH.l_user_group_activity CASCADE;

CREATE TABLE IF NOT EXISTS VT260224AD30FB__DWH.l_user_group_activity
(
    hk_l_user_group_activity BIGINT PRIMARY KEY,
    hk_user_id               BIGINT NOT NULL
        CONSTRAINT fk_l_user_group_activity_h_users
            REFERENCES VT260224AD30FB__DWH.h_users (hk_user_id),
    hk_group_id              BIGINT NOT NULL
        CONSTRAINT fk_l_user_group_activity_h_groups
            REFERENCES VT260224AD30FB__DWH.h_groups (hk_group_id),
    load_dt                  DATETIME,
    load_src                 VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)
;
