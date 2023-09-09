from .AbstractPreprocessor import BaseDataProcessor


class ConstantImputeProcessor(BaseDataProcessor):
    def __init__(self, metric_settings, data, value):
        super().__init__(metric_settings, data)
        self.value = value

    def preprocess(self):
        impute_value = self.metric_settings.get('impute_value', None)
        if not impute_value:
            pass
        else:
            self.data['value'] = self.data['value'].fillna(impute_value)
