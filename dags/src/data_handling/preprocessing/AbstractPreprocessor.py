from abc import ABC, abstractmethod


class BaseDataProcessor(ABC):
    def __init__(self,
                 metric_settings,
                 data):
        self.metric_settings = metric_settings
        self.data = data

    def preprocess(self):
        raise NotImplementedError