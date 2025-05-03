import logging
from collections import defaultdict
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    as_completed,
)
from functools import cached_property

import boto3
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from celery import Task as CeleryTask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib3.filepost import writer

from .decoder import Decoder
from .extractors import (
    Extractor,
    FHV13Extractor,
    GLCMExtractor,
    GLCMPropertyExtractor,
    SIExtractor,
    TICalculator,
    YExtractor,
)
from .features import (
    FHV13Calculator,
    FeatureCalculator,
    MeanCalculator,
    STDCalculator,
)

Processor = Extractor | FeatureCalculator
ProcessorResult = np.ndarray | float | None


def run_processor(processor: Processor, future: Future[np.ndarray]) -> tuple[Processor, ProcessorResult]:
    if isinstance(processor, Extractor):
        return processor, processor.extract(future.result())

    return processor, processor.feed_frame(future.result())


class FeatureCalculatorTask(CeleryTask):
    CRFS = tuple(range(16, 30))
    QP = tuple(range(25, 50))

    PARAMS = [
        {'parameter': 'crf', 'value': crf}
        for crf in CRFS
    ] + [
        {'parameter': 'qp', 'value': qp}
        for qp in QP
    ]

    @cached_property
    def s3_client(self):
        s3_access_key_id = self.app.conf.get('s3_access_key_id')
        s3_secret_access_key = self.app.conf.get('s3_secret_access_key')
        s3_endpoint_url = self.app.conf.get('s3_endpoint_url')

        if not all([s3_access_key_id, s3_secret_access_key]):
            raise ValueError("S3 credentials not configured in worker")

        return boto3.client(
            's3',
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key,
        )

    @property
    def s3_bucket(self):
        return self.app.conf.get('s3_bucket')

    @cached_property
    def regressor_model(self) -> CatBoostRegressor:
        regressor = CatBoostRegressor()
        regressor.load_model(self.app.conf.get('model.cbm'))
        return regressor

    @staticmethod
    def analyze_file(presigned_url: str) -> list[dict]:
        extractors = [
            FHV13Extractor(),
            GLCMExtractor(),
            GLCMPropertyExtractor('contrast'),
            GLCMPropertyExtractor('correlation'),
            GLCMPropertyExtractor('energy'),
            GLCMPropertyExtractor('homogeneity'),
            SIExtractor(),
            TICalculator(),
            YExtractor(),
        ]
        feature_calculators = [
            MeanCalculator('Y', 'CTI_mean'),
            STDCalculator('Y', 'CTI_std'),
            FHV13Calculator(),
            MeanCalculator('GLCM_contrast'),
            STDCalculator('GLCM_contrast'),
            MeanCalculator('GLCM_correlation'),
            STDCalculator('GLCM_correlation'),
            MeanCalculator('GLCM_energy'),
            STDCalculator('GLCM_energy'),
            MeanCalculator('GLCM_homogeneity'),
            STDCalculator('GLCM_homogeneity'),
            MeanCalculator('SI'),
            STDCalculator('SI'),
            MeanCalculator('TI'),
            STDCalculator('TI'),
        ]
        processors = extractors + feature_calculators
        results = []
        with Decoder(presigned_url) as decoder:
            with ThreadPoolExecutor() as pool:
                for frame in decoder:
                    item = {
                        'width': frame.width,
                        'height': frame.height,
                    }
                    extractors_results = defaultdict(Future)
                    frame_data = frame.to_ndarray()
                    extractors_results[None].set_result(frame_data)
                    futures = []
                    for processor in processors:
                        matrix_future = extractors_results[processor.depends_on()]
                        futures.append(pool.submit(run_processor, processor, matrix_future))

                    for future in as_completed(futures):
                        calc, result = future.result()
                        if isinstance(calc, Extractor):
                            extractors_results[calc.name()].set_result(result)
                        else:
                            item[calc.name()] = result
                    results.append(item)
        return result

    @staticmethod
    def select_best_row(dataframe) -> dict:
        high_quality_rows = dataframe[dataframe['quality'] >= 95]

        if not high_quality_rows.empty:
            return {'parameter': 'crf', 'value': 16}

        row = dataframe.loc[dataframe['quality'].idxmin()]
        return {'parameter': row['parameter'], 'value': row['value']}

    def predict_parameters(self, df: pd.DataFrame) -> dict:
        predicted_metrics = self.regressor_model.predict(df)
        df['quality'] = predicted_metrics
        return self.select_best_row(df)

    def run(self, source_path: str) -> dict:
        source_path = source_path.lstrip('/')
        presigned_url = self.s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.s3_bucket,
                'Key': source_path,
            },
            ExpiresIn=3600 * 24,
        )
        logging.info(f'Analyzing file {self.s3_bucket}/{source_path}')
        try:
            data = self.analyze_file(presigned_url)
        except Exception as e:
            logging.exception(f'Error while analyzing file {self.s3_bucket}/{source_path}: {e}')
            return {'error': str(e), 'status': 'failed'}

        dataframe = pd.DataFrame(data)
        big_features = dataframe.agg(
            {
                'width': 'min',
                'height': 'min',
                'CTI_mean': ['min', 'mean', 'max', 'std'],
                'CTI_std': ['min', 'mean', 'max', 'std'],
                'FHV13': ['max'],
                'GLCM_contrast_mean': ['min', 'mean', 'max', 'std'],
                'GLCM_contrast_std': ['std'],
                'GLCM_correlation_mean': ['min', 'mean', 'max', 'std'],
                'GLCM_correlation_std': ['std'],
                'GLCM_energy_mean': ['min', 'mean', 'max', 'std'],
                'GLCM_energy_std': ['min', 'mean', 'max', 'std'],
                'GLCM_homogeneity_mean': ['min', 'mean', 'max', 'std'],
                'GLCM_homogeneity_std': ['min', 'mean', 'max', 'std'],
                'SI_mean': ['min', 'mean', 'max', 'std'],
                'SI_std': ['min', 'mean', 'max', 'std'],
                'TI_mean': ['max', 'std'],
                'TI_std': ['min', 'mean', 'max', 'std'],
            }
        )
        big_features.columns = big_features.columns.map('_'.join)
        X_data = pd.DataFrame(self.PARAMS).merge(big_features, how='cross')
        return self.predict_parameters(X_data)
