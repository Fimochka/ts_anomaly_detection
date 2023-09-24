import pandas as pd
import datetime as dt
import psycopg2
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

from .AbstractDataLoader import BaseDataLoader


class PostgresqlDataLoader(BaseDataLoader):
    def __init__(self, run_dttm, metric_settings):
        super().__init__(run_dttm, metric_settings)
        self.metric_settings = metric_settings
        self.run_dttm = run_dttm
        self.df = None

    def _init_connection(self,
                         connection_id):
        self.connection_id = connection_id
        # init postgreshook
        self.postgres_hook = PostgresHook(postgres_conn_id=connection_id)

    def load_data(self,
                  **kwargs):
        self._init_connection(connection_id=kwargs.get('connection_id'))
        # variables
        input_data_table = kwargs.get('input_data_table', None)
        metric_name = kwargs.get('metric_name', None)
        start_dttm = kwargs.get('start_dttm', None)

        query = """select *
                   from {input_data_table}
                   where metric_name='{metric_name}' 
                   and event_dttm between '{start_dttm}' and '{event_dttm}'""".format(input_data_table=input_data_table,
                                                                                      metric_name=metric_name,
                                                                                      event_dttm=self.run_dttm,
                                                                                      start_dttm=start_dttm)
        logging.info(query)
        self.df = self.postgres_hook.get_pandas_df(sql=query)

        if kwargs.get('is_preprocessing', None):
            self._prepare_data()

    def _prepare_data(self):
        metric_name = self.metric_settings.get('metric_name')
        # preprocess date column
        date_column_name = self.metric_settings.get('date_column_name')
        date_column_format = self.metric_settings.get('date_column_format')
        self.df[date_column_name] = self.df[date_column_name].apply(lambda x: dt.datetime.strptime(x,
                                                                                                   date_column_format),
                                                                    1)
        # rename columns
        self.df = self.df[[date_column_name,
                           metric_name]]

        self.df.columns = ['event_dttm', 'value']
        self.df['metric_name'] = metric_name

    def upload_data(self,
                    **kwargs):
        data = kwargs.get('data', None)
        connection_id = kwargs.get('connection_id', None)
        output_table = kwargs.get('output_table', None)
        # init postgreshook
        postgres_hook = PostgresHook(postgres_conn_id=connection_id)
        # insert data
        postgres_hook.insert_rows(output_table,
                                  data.values)
