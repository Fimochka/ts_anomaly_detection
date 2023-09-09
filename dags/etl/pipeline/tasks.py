import sys
import logging
import pandas as pd

from src.data_handling.loaders.CSVDataLoader import *
from src.data_handling.preprocessing.ConstantImputeProcessor import *


def load_data(event_dttm,
              metric_settings,
              **kwargs):
    metric_name = metric_settings.get('metric_name')
    data_loader = metric_settings.get('data_loader')
    print(sys.path)
    logging.info("Metric: {metric_name}, DataLoader: {data_loader}".format(metric_name=metric_name,
                                                                           data_loader=data_loader))
    # getting data loader class for this metric
    loader_class = getattr(sys.modules[__name__], data_loader)
    loader = loader_class(event_dttm=event_dttm,
                          metric_settings=metric_settings)
    # load data
    loader.load_data()
    return loader.df.to_json()


def preprocess_data(metric_settings,
                    load_task_id,
                    **kwargs):
    ti = kwargs['ti']
    data = pd.read_json(ti.xcom_pull(task_ids=load_task_id))
    print(data)
    metric_name = metric_settings.get('metric_name', None)
    data_preprocessor = metric_settings.get('data_preprocessor', None)
    print(sys.path)
    logging.info("Metric: {metric_name}, Preprocessor: {data_preprocessor}".format(metric_name=metric_name,
                                                                                   data_preprocessor=data_preprocessor))
