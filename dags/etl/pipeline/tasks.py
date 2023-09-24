import sys
import logging
import pandas as pd
import datetime as dt
import psycopg2

from src.data_handling.loaders.CSVDataLoader import *
from src.data_handling.loaders.PostgresqlDataLoader import *
from src.data_handling.preprocessing.ConstantImputeProcessor import *


def get_metric_list(metrics: dict = None,
                    connection: psycopg2.connect = None) -> dict:
    return metrics

def load_data(run_dttm: str = None,
              metric_settings: dict = None,
              connection: psycopg2.connect = None) -> pd.DataFrame:

    metric_name = metric_settings.get('metric_name')
    data_loader = metric_settings.get('data_loader')
    logging.info(run_dttm)
    logging.info("Metric: {metric_name}, DataLoader: {data_loader}".format(metric_name=metric_name,
                                                                           data_loader=data_loader))
    # getting data loader class for this metric
    loader_class = getattr(sys.modules[__name__], data_loader)
    loader = loader_class(run_dttm=run_dttm,
                          metric_settings=metric_settings)
    # load data
    loader.load_data(connection=connection)
    return loader.df.to_json()


def preprocess_data(metric_settings: dict = None,
                    load_task_id: str = None,
                    **kwargs) -> pd.DataFrame:

    ti = kwargs['ti']
    data = pd.read_json(ti.xcom_pull(task_ids=load_task_id))
    metric_name = metric_settings.get('metric_name', None)
    data_preprocessor = metric_settings.get('data_preprocessor', None)
    logging.info("Metric: {metric_name}, Preprocessor: {data_preprocessor}".format(metric_name=metric_name,
                                                                                   data_preprocessor=data_preprocessor))
    if not data_preprocessor:
        return data.to_json()

    # getting data preprocessor class for this metric
    preprocessor_class = getattr(sys.modules[__name__], data_preprocessor.get('class_name', None))
    preprocessor = preprocessor_class(metric_settings=metric_settings,
                                      data=data,
                                      value=data_preprocessor.get('impute_value'))
    # preprocess data
    preprocessor.preprocess()
    print(preprocessor.data)
    return preprocessor.data.to_json()


def upload_data(run_dttm: str = None,
                metric_settings: dict = None,
                preprocess_task_id: str = None,
                connection_id: str = None,
                output_table: str = None,
                **kwargs) -> None:
    ti = kwargs['ti']
    # get data
    data = pd.read_json(ti.xcom_pull(task_ids=preprocess_task_id))
    # init data loader class for this metric
    loader = PostgresqlDataLoader(run_dttm=run_dttm,
                                  metric_settings=metric_settings)
    # upload to an internal database
    loader.upload_data(data=data,
                       connection_id=connection_id,
                       output_table=output_table)

    return
