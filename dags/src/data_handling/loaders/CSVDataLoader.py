import pandas as pd
import datetime as dt
import logging

from .AbstractDataLoader import BaseDataLoader


class CSVDataLoader(BaseDataLoader):
    def __init__(self, run_dttm, metric_settings):
        super().__init__(run_dttm, metric_settings)
        self.metric_settings = metric_settings
        self.run_dttm = run_dttm
        self.df = None

    def _init_connection(self,
                         connection):
        pass

    def load_data(self,
                  **kwargs):
        data_path = self.metric_settings.get('data_path', None)
        self.df = pd.read_csv(data_path)
        self._prepare_data()

    def _prepare_data(self):
        metric_name = self.metric_settings.get('metric_name')
        # preprocess date column
        date_column_name = self.metric_settings.get('date_column_name')
        date_column_format = self.metric_settings.get('date_column_format')
        self.df[date_column_name] = self.df[date_column_name].apply(lambda x: dt.datetime.strptime(x,
                                                                                                   date_column_format), 1)
        self.df[date_column_name] = self.df[date_column_name].apply(lambda x: dt.datetime.strftime(x,
                                                                                                   "%Y-%m-%dT%H:%M:%S+00:00"),
                                                                    1)
        # rename columns
        self.df = self.df[[date_column_name,
                           metric_name]]

        self.df.columns = ['event_dttm', 'value']
        self.df['metric_name'] = metric_name

        # filtering data by run_dttm
        self.df = self.df[self.df['event_dttm'] == self.run_dttm]

    def upload_data(self,
                    **kwargs):
        pass
