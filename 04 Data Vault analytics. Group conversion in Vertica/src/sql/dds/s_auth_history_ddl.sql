-- DDL сателлита s_auth_history в слое DDS

DROP TABLE IF EXISTS VT260224AD30FB__DWH.s_auth_history;

CREATE TABLE IF NOT EXISTS VT260224AD30FB__DWH.s_auth_history
(
    hk_l_user_group_activity BIGINT NOT NULL
        CONSTRAINT fk_s_auth_history_l_user_group_activity
            REFERENCES VT260224AD30FB__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from             BIGINT,
    event                    VARCHAR(6),
    event_dt                 TIMESTAMP(0),
    load_dt                  DATETIME,
    load_src                 VARCHAR(20)
)
;
