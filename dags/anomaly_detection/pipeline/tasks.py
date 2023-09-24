import sys
import logging
import pandas as pd
import datetime as dt
import psycopg2

from src.data_handling.loaders.CSVDataLoader import *
from src.data_handling.loaders.PostgresqlDataLoader import *
from src.data_handling.preprocessing.ConstantImputeProcessor import *
from src.anomaly_detection.IQRAnomalyDetector import *


def get_metric_list(metrics: dict = None,
                    connection: psycopg2.connect = None) -> dict:
    return metrics

def load_data(run_dttm: str = None,
              metric_settings: dict = None,
              connection_id: str = None,
              input_data_table: str = None) -> pd.DataFrame:

    metric_name = metric_settings.get('metric_name')
    data_loader = metric_settings.get('data_loader')
    logging.info(metric_settings)
    logging.info("Metric: {metric_name}, DataLoader: {data_loader}".format(metric_name=metric_name,
                                                                           data_loader=data_loader))
    # getting data loader class for this metric
    loader_class = getattr(sys.modules[__name__], data_loader)
    loader = loader_class(run_dttm=run_dttm,
                          metric_settings=metric_settings)

    # define start_dttm
    train_period = metric_settings.get('kwargs').get('train_period')
    start_dttm = dt.datetime.strptime(run_dttm, '%Y-%m-%dT%H:%M:%S+00:00') - dt.timedelta(days=train_period)
    start_dttm = dt.datetime.strftime(start_dttm, '%Y-%m-%dT%H:%M:%S+00:00')
    # load data
    loader.load_data(connection_id=connection_id,
                     input_data_table=input_data_table,
                     metric_name=metric_name,
                     start_dttm=start_dttm)
    return loader.df.to_json()


def detect_anomalies(run_dttm: str = None,
                     metric_settings: dict = None,
                     load_task_id: str = None,
                     **kwargs) -> pd.DataFrame:

    ti = kwargs['ti']
    data = pd.read_json(ti.xcom_pull(task_ids=load_task_id))
    metric_name = metric_settings.get('metric_name', None)
    anomaly_detector = metric_settings.get('anomaly_detector', None)
    logging.info("Metric: {metric_name}, Anomaly Detector: {anomaly_detector}".format(metric_name=metric_name,
                                                                                      anomaly_detector=anomaly_detector))

    # getting anomaly_detector class for this metric
    anomaly_detector_class = getattr(sys.modules[__name__], anomaly_detector)
    detector = anomaly_detector_class(run_dttm=run_dttm,
                                      metric_settings=metric_settings)
    # define start_dttm
    train_period = metric_settings.get('kwargs').get('train_period')
    start_dttm = dt.datetime.strptime(run_dttm, '%Y-%m-%dT%H:%M:%S+00:00') - dt.timedelta(days=train_period)
    start_dttm = dt.datetime.strftime(start_dttm, '%Y-%m-%dT%H:%M:%S+00:00')
    # find anomalies
    detector.fit(data=data[data['event_dttm']<run_dttm])
    result_data = detector.predict(data=data[data['event_dttm']==run_dttm])
    return result_data.to_json()


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