import sys
import airflow
import yaml
import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from etl.pipeline import tasks

logging.info("Reading config...")
with open("./dags/etl/settings/config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

metrics = cfg.get('metrics')

postgreshook = PostgresHook(conn_name_attr='internal_postgresql_database')
conn = postgreshook.get_conn()
print(conn)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(dag_id="ts_anomaly_detector_etl",
         schedule_interval="@hourly",
         max_active_tasks=1,
         max_active_runs=2,
         start_date=datetime(2023, 3, 1),
         end_date=datetime(2023, 3, 2)) as dag:
    create_psql_table = PostgresOperator(
        task_id="create_raw_data_table_if_not_exists",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS {output_table} (
                event_dttm              VARCHAR,
                value                   FLOAT,
                metric_name             VARCHAR
                );
            """.format(output_table=cfg.get('raw_data_table')))

    get_metric_list_task = PythonOperator(
        task_id ='get_metric_list_task',
        op_kwargs={'metrics': metrics,
                   'connection': conn},
        python_callable=tasks.get_metric_list,
        provide_context=True,
        dag=dag)

    select_psql_table = PostgresOperator(
        task_id="select_raw_data_table",
        postgres_conn_id="postgres_default",
        sql="""
                select *
                from {output_table} 
                  ;
                """.format(output_table=cfg.get('raw_data_table')))

    for i, metric_settings in enumerate(metrics):
        metric_name = metric_settings.get('metric_name')
        data_loader = metric_settings.get('data_loader')

        load_task = PythonOperator(
            task_id=f'load_task_{metric_name}',
            op_kwargs={'run_dttm':'{{ ts }}',
                       'metric_settings': metric_settings,
                       'connection': conn},
            python_callable=tasks.load_data,
            provide_context=True,
            dag=dag)

        preprocess_task = PythonOperator(
            task_id=f'preprocess_task_{metric_name}',
            op_kwargs={'metric_settings': metric_settings,
                       'load_task_id': f'load_task_{metric_name}'},
            python_callable=tasks.preprocess_data,
            provide_context=True,
            dag=dag)

        drop_partition_if_exists = PostgresOperator(
            task_id= f'drop_if_exists_task_{metric_name}',
            postgres_conn_id="postgres_default",
            sql="""
                    delete from {output_table} 
                    where metric_name='{metric_name}' 
                      and event_dttm='{event_dttm}';
                    """.format(metric_name=metric_name,
                               event_dttm='{{ ts }}',
                               output_table=cfg.get('raw_data_table')))

        upload_task = PythonOperator(
            task_id=f'upload_task_{metric_name}',
            op_kwargs={'event_dttm':'{{ ts }}',
                       'metric_settings': metric_settings,
                       'preprocess_task_id': f'preprocess_task_{metric_name}',
                       'connection_id': 'postgres_default',
                       'output_table': cfg.get('raw_data_table')},
            python_callable=tasks.upload_data,
            provide_context=True,
            dag=dag)

        create_psql_table >> get_metric_list_task >> \
        load_task >> preprocess_task >> drop_partition_if_exists >> upload_task >> select_psql_table
