import logging
from concurrent.futures import (
    FIRST_COMPLETED,
    ThreadPoolExecutor,
    wait,
)
from functools import cached_property
from graphlib import TopologicalSorter
from itertools import chain

import boto3
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from celery import Task as CeleryTask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database import Task, TaskStatus
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


def run_processor(processor: Processor, frame_data: np.ndarray) -> tuple[Processor, ProcessorResult]:
    if isinstance(processor, Extractor):
        return processor, processor.extract(frame_data)

    return processor, processor.feed_frame(frame_data)


class FeatureCalculatorTask(CeleryTask):
    name = 'feature_calculator'
    CRFS = tuple(range(17, 31))
    QP = tuple(range(25, 41))
    PROGRESS_INTERVAL = 25

    PARAMS = [
        {'parameter': 'crf', 'value': crf}
        for crf in CRFS
    ] + [
        {'parameter': 'qp', 'value': qp}
        for qp in QP
    ]
    logger = logging.getLogger('feature_calculator')

    @cached_property
    def session_maker(self):
        engine = create_engine(self.app.conf.get('database_url'))
        return sessionmaker(engine, expire_on_commit=False)

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
        regressor.load_model(self.app.conf.get('regressor_path'))
        return regressor

    @classmethod
    def analyze_file(cls, presigned_url: str) -> list[dict]:
        single_extractors = [
            YExtractor(),
            TICalculator(),
        ]
        extractors = [
            FHV13Extractor(),
            GLCMExtractor(),
            GLCMPropertyExtractor('contrast'),
            GLCMPropertyExtractor('correlation'),
            GLCMPropertyExtractor('energy'),
            GLCMPropertyExtractor('homogeneity'),
            SIExtractor(),
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
        processors = {
            processor.name(): processor
            for processor in chain(single_extractors, extractors, feature_calculators)
        }
        dependencies = {
            processor.name(): [processor.depends_on()] if processor.depends_on() else []
            for processor in chain(extractors, feature_calculators)
        }
        pending = set()
        rows = []
        with Decoder(presigned_url) as decoder, ThreadPoolExecutor() as pool:
            duration = decoder.video_stream.duration
            for idx, frame in enumerate(decoder):
                if idx % cls.PROGRESS_INTERVAL == 0:
                    progress = frame.pts / duration * 100
                    cls.logger.info(f'Progress: {progress:.2f}%')

                item = {
                    'width': frame.width,
                    'height': frame.height,
                }
                extractors_results = {}
                frame_data = frame.to_ndarray()

                extractors_results[None] = frame_data
                for extractor in single_extractors:
                    extractors_results[extractor.name()] = extractor.extract(frame_data)

                future = pool.submit(
                    cls.process_one_frame,
                    dependencies,
                    extractors_results,
                    item,
                    processors
                )
                pending.add(future)
                if len(pending) > 10:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        rows.append(future.result())

        for future in pending:
            rows.append(future.result())

        return rows

    @staticmethod
    def process_one_frame(
        dependencies: dict[str, list],
        extractors_results: dict[str, np.ndarray],
        item,
        processors,
    ):
        with ThreadPoolExecutor() as pool:
            graph = TopologicalSorter(dependencies)
            graph.prepare()
            pending = set()
            while graph.is_active():
                ready_processors_names = graph.get_ready()
                for name in ready_processors_names:
                    processor = processors[name]
                    matrix = extractors_results[processor.depends_on()]
                    future = pool.submit(run_processor, processor, matrix)
                    pending.add(future)

                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    calc, result = future.result()
                    graph.done(calc.name())
                    if isinstance(calc, Extractor):
                        extractors_results[calc.name()] = result
                    else:
                        item[calc.name()] = result
        return item

    @staticmethod
    def select_best_row(dataframe) -> dict:
        high_quality_rows = dataframe[dataframe['quality'] >= 95]

        if high_quality_rows.empty:
            return {'parameter': 'crf', 'value': 16}

        row = dataframe.loc[high_quality_rows['quality'].idxmin()]
        return {'parameter': row['parameter'], 'value': int(row['value'])}

    def predict_parameters(self, df: pd.DataFrame) -> dict:
        predicted_metrics = self.regressor_model.predict(df[self.regressor_model.feature_names_])
        df['quality'] = predicted_metrics
        return self.select_best_row(df)

    def run(self, task_id: int, source_path: str) -> dict:
        with self.session_maker.begin() as session:
            task = session.query(Task).filter_by(
                id=task_id,
            ).first()

            if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                self.logger.error(f'Task {task.id} is finished')
                return {'error': 'Task is finished', 'status': 'failed'}

            task.status = TaskStatus.PROCESSING
            session.commit()

        source_path = source_path.lstrip('/')
        presigned_url = self.s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.s3_bucket,
                'Key': source_path,
            },
            ExpiresIn=3600 * 24,
        )
        self.logger.info(f'Analyzing file {self.s3_bucket}/{source_path}')
        try:
            data = self.analyze_file(presigned_url)
        except Exception as e:
            self.logger.exception(f'Error while analyzing file {self.s3_bucket}/{source_path}: {e}')
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
        dts = big_features.unstack().dropna()
        big_features = pd.DataFrame(
            [dts.values],
            columns=map('_'.join, dts.index),
        )
        self.logger.info(f'Features {big_features.to_dict("records")}')
        X_data = pd.DataFrame(self.PARAMS).merge(big_features, how='cross')
        predicted_parameters = self.predict_parameters(X_data)
        predicted_parameters['status'] = 'success'
        self.logger.info(f'{predicted_parameters=}')
        return predicted_parameters
