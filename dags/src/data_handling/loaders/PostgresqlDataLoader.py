import pandas as pd
import datetime as dt
import psycopg2

from .AbstractDataLoader import BaseDataLoader


class PostgresqlDataLoader(BaseDataLoader):
    def __init__(self, event_dttm, metric_settings):
        super().__init__(event_dttm, metric_settings)
        self.metric_settings = metric_settings
        self.event_dttm = event_dttm
        self.df = None

    def _init_connection(self,
                         connection):
        self.connection = connection

    def load_data(self,
                  **kwargs):
        self._init_connection(connection=kwargs.get('connection'))
        query = kwargs.get('query', None)
        self.df = pd.read_csv(data_path)
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

        # filtering hostory if needed
        start_dttm = self.metric_settings.get('start_dttm', None)
        if start_dttm:
            self.df = self.df[self.df['event_dttm'] > start_dttm]

    def upload_data(self):
        pass