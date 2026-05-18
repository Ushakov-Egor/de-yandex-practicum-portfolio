import pendulum
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
import shutil
import os

import boto3

AWS_ACCESS_KEY_ID = "your_access_key_id"        # замените на реальный ключ из Yandex Cloud IAM
AWS_SECRET_ACCESS_KEY = "your_secret_access_key"  # замените на реальный секрет из Yandex Cloud IAM


# выводим первые 10 строк каждого файла из списка
bash_command_tmpl = """
for f in {{ params.files | join(' ') }}; do
    echo "=== $f ===";
    head -n 10 "$f";
done
"""

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'), catchup=False)
def sprint6_dag_get_data_files():
    bucket_files = ['users.csv', 'groups.csv', 'dialogs.csv', 'group_log.csv']

    @task()
    def fetch_s3_files():
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        local_dir = '/src/data'
        os.makedirs(local_dir, exist_ok=True)

        for file in bucket_files:
            local_path = f'{local_dir}/{file}'
            s3_client.download_file(
                Bucket='sprint6',
                Key=file,
                Filename=local_path,
            )
            print(f'Saved to: {local_path}')

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': [f'/src/data/{f}' for f in bucket_files]}
    )

    fetch_task = fetch_s3_files()

    fetch_task >> print_10_lines_of_each

_ = sprint6_dag_get_data_files()
