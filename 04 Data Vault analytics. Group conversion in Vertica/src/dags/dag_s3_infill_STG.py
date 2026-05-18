import pendulum
import vertica_python
from airflow.decorators import dag, task


conn_info = {
    'host': 'vertica.data-engineer.education-services.ru',
    'port': 5433,  # стандартный порт Vertica
    'user': 'vt260224ad30fb',
    'password': 'your_vertica_password',  # задать через переменную окружения
    'database': 'dwh',
    'autocommit': True,  # или False
}


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'), catchup=False)
def sprint6_infill_STG():

    @task
    def t_create_STG_tables():
        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute('''
                DROP TABLE IF EXISTS VT260224AD30FB__STAGING.users CASCADE;
                CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.users (
                    id               INT          NOT NULL,
                    chat_name        VARCHAR(200) NOT NULL,
                    registration_dt  TIMESTAMP(6),
                    country          VARCHAR(200),
                    age              INT,
                    CONSTRAINT pk_users PRIMARY KEY (id)
                )
                ORDER BY id
                ;
            ''')
            cur.execute('''
                DROP TABLE IF EXISTS VT260224AD30FB__STAGING.groups CASCADE;
                CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.groups (
                    id               INT          NOT NULL,
                    admin_id         INT,
                    group_name       VARCHAR(200),
                    registration_dt  TIMESTAMP(6),
                    is_private       INT NULL, -- CHECK (is_private IN (0, 1))
                    CONSTRAINT pk_groups PRIMARY KEY (id),
                    CONSTRAINT fr_groups_to_users FOREIGN KEY (admin_id) REFERENCES VT260224AD30FB__STAGING.users (id)
                )
                ORDER BY id, admin_id
                PARTITION BY registration_dt::date
                GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2)
                ;
            ''')
            cur.execute('''
                DROP TABLE IF EXISTS VT260224AD30FB__STAGING.dialogs CASCADE;
                CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.dialogs (
                    message_id    INT           NOT NULL,
                    message_ts    TIMESTAMP(6),
                    message_from  INT,
                    message_to    INT,
                    message       VARCHAR(1000),
                    message_group INT,
                    CONSTRAINT pk_dialogs PRIMARY KEY (message_id),
                    CONSTRAINT fr_message_from_to_users FOREIGN KEY (message_from) REFERENCES VT260224AD30FB__STAGING.users (id),
                    CONSTRAINT fr_message_to_to_users FOREIGN KEY (message_to) REFERENCES VT260224AD30FB__STAGING.users (id)
                    -- CONSTRAINT fr_dialogs_to_groups FOREIGN KEY (message_group) REFERENCES VT260224AD30FB__STAGING.groups (id)
                )
                ORDER BY message_id
                PARTITION BY message_ts::date
                GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2)
                ;
            ''')
            cur.execute('''
                DROP TABLE IF EXISTS VT260224AD30FB__STAGING.group_log;
                CREATE TABLE IF NOT EXISTS VT260224AD30FB__STAGING.group_log (
                    group_id    INT         NOT NULL,
                    user_id     INT         NOT NULL,
                    user_id_from INT,
                    event       VARCHAR(6),
                    datetime    DATETIME,
                    CONSTRAINT pk_group_log PRIMARY KEY (group_id)
                )
                ORDER BY group_id
                PARTITION BY datetime::date
                GROUP BY calendar_hierarchy_day(datetime::date, 3, 2)
                ;
            ''')


    @task()
    def t_infill_users_STG():
        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute("""
                COPY VT260224AD30FB__STAGING.users (id, chat_name, registration_dt, country, age)
                FROM LOCAL '/src/data/users.csv'
                DELIMITER ','
                SKIP 1
                REJECTMAX 100
                REJECTED DATA AS TABLE VT260224AD30FB__STAGING.users_rej
                ;
            """)

    @task()
    def t_infill_groups_STG():
        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute("""
                COPY VT260224AD30FB__STAGING.groups (id, admin_id, group_name, registration_dt, is_private)
                FROM LOCAL '/src/data/groups.csv'
                DELIMITER ','
                SKIP 1
                REJECTMAX 100
                REJECTED DATA AS TABLE VT260224AD30FB__STAGING.groups_rej
                ;
            """)

    @task()
    def t_infill_dialogs_STG():
        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute("""
                COPY VT260224AD30FB__STAGING.dialogs (message_id, message_ts, message_from, message_to, message, message_group)
                FROM LOCAL '/src/data/dialogs.csv'
                DELIMITER ','
                ENCLOSED BY '"'
                SKIP 1
                REJECTED DATA AS TABLE VT260224AD30FB__STAGING.dialogs_rej
                ;
            """)

    @task()
    def t_infill_group_log_STG():
        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute("""
                COPY VT260224AD30FB__STAGING.group_log (group_id, user_id, user_id_from, event, datetime)
                FROM LOCAL '/src/data/group_log.csv'
                DELIMITER ','
                SKIP 1
                NULL AS ''
                REJECTMAX 100
                REJECTED DATA AS TABLE VT260224AD30FB__STAGING.group_log_rej
                ;
            """)

    t_create_STG_tables = t_create_STG_tables()
    t_infill_users_STG = t_infill_users_STG()
    t_infill_groups_STG = t_infill_groups_STG()
    t_infill_dialogs_STG = t_infill_dialogs_STG()
    t_infill_group_log_STG = t_infill_group_log_STG()

    t_create_STG_tables >> t_infill_users_STG >> t_infill_groups_STG >> t_infill_dialogs_STG >> t_infill_group_log_STG


_ = sprint6_infill_STG()
