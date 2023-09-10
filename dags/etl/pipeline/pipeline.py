import sys
import airflow
import yaml
import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.pipeline import tasks

logging.info("Reading config...")
with open("./dags/etl/settings/config.yaml", "r") as f:
    metrics = yaml.safe_load(f).get('metrics')

postgreshook = PostgresHook(conn_name_attr= 'internal_postgresql_database')
conn = postgreshook.get_conn()
print(conn)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_task(event_dttm,
               metric_settings):
    print(event_dttm)
    print(metric_settings)


with DAG(dag_id="ts_anomaly_detector_etl",
         schedule_interval=None, # "@daily",
         max_active_tasks=1,
         max_active_runs=2,
         start_date=datetime(2023, 1, 1)) as dag:

    get_metric_list_task = PythonOperator(
        task_id ='get_metric_list_task',
        op_kwargs={'metrics': metrics,
                   'connection': conn},
        python_callable=tasks.get_metric_list,
        provide_context=True,
        dag=dag)

    for i, metric_settings in enumerate(metrics):
        metric_name = metric_settings.get('metric_name')
        data_loader = metric_settings.get('data_loader')

        load_task_id = f'load_task_{metric_name}'
        preprocess_task_id = f'preprocess_task_{metric_name}'
        upload_task_id = f'upload_task_{metric_name}'

        load_task = PythonOperator(
            task_id=load_task_id,
            op_kwargs={'event_dttm':'{{ ds }}',
                       'metric_settings': metric_settings,
                       'connection': conn},
            python_callable=tasks.load_data,
            provide_context=True,
            dag=dag)

        preprocess_task = PythonOperator(
            task_id=preprocess_task_id,
            op_kwargs={'metric_settings': metric_settings,
                       'load_task_id': load_task_id},
            python_callable=tasks.preprocess_data,
            provide_context=True,
            dag=dag)

        upload_task = PythonOperator(
            task_id=upload_task_id,
            op_kwargs={'metric_settings': metric_settings,
                       'preprocess_task_id': preprocess_task_id,
                       'connection': conn},
            python_callable=tasks.upload_data,
            provide_context=True,
            dag=dag)

        get_metric_list_task >> load_task >> preprocess_task >> upload_task
