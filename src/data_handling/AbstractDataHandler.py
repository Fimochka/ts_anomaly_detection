from abc import ABC, abstractmethod


class BaseDataHandler(ABC):
    def __init__(self,
                 connection,
                 event_dttm):
        self.connection = connection
        self.event_dttm = event_dttm

    def load_data(self):
        raise NotImplementedError

    def transform(self,
                  data,
                  method):
        raise NotImplementedError

    def insert(self):
        raise NotImplementedError