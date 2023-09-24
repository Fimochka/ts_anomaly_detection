from abc import ABC, abstractmethod


class BaseAnomalyDetector(ABC):
    def __init__(self,
                 run_dttm,
                 metric_settings):
        self.metric_settings = metric_settings
        self.run_dttm = run_dttm

    def fit(self,
            **kwargs):
        raise NotImplementedError

    def predict(self,
                **kwargs):
        raise NotImplementedError
