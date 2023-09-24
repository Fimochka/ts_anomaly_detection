import pandas as pd
import datetime as dt
import logging

from .AbstractAnomalyDetector import BaseAnomalyDetector


class IQRAnomalyDetector(BaseAnomalyDetector):
    def __init__(self,
                 run_dttm,
                 metric_settings):
        self.metric_settings = metric_settings
        self.run_dttm = run_dttm

    def fit(self,
            **kwargs):
        IQR_coef = self.metric_settings.get('kwargs').get('IQR_coef', 1.5)
        metric_name = self.metric_settings.get('metric_name')
        data = kwargs.get('data')
        q25 = data['value'].quantile(0.25)
        q75 = data['value'].quantile(0.75)
        iqr = q75 - q25
        self.up = q75+ IQR_coef * iqr
        self.low = q25 - IQR_coef * iqr

    def predict(self,
                **kwargs):
        data = kwargs.get('data')
        data['high_th_value'] = self.up
        data['low_th_value'] = self.low
        data['anomaly_detection_method'] = self.metric_settings.get('anomaly_detector')
        data['event_dttm'] = self.run_dttm
        return data[['event_dttm', 'value', 'metric_name',
                     'low_th_value', 'high_th_value', 'anomaly_detection_method']]
