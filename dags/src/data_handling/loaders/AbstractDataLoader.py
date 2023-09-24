from abc import ABC, abstractmethod


class BaseDataLoader(ABC):
    def __init__(self,
                 event_dttm,
                 metric_settings):
        self.metric_settings = metric_settings
        self.event_dttm = event_dttm

    def _init_connection(self,
                         connection):
        raise NotImplementedError

    def load_data(self,
                  **kwargs):
        raise NotImplementedError

    def upload_data(self,
                    **kwargs):
        raise NotImplementedError
