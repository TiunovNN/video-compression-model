import csv
import io
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from graphlib import TopologicalSorter
from itertools import chain

import boto3
import click
import numpy as np
from dotenv import load_dotenv

from decoder import Decoder
from extractors import (
    Extractor,
    GLCMExtractor,
    GLCMPropertyExtractor,
    SIExtractor,
    TICalculator,
    YExtractor,
)
from features import FeatureCalculator, MeanCalculator, STDCalculator

Processor = Extractor | FeatureCalculator
ProcessorResult = np.ndarray | float | None


def run_processor(processor: Processor, frame: np.ndarray) -> tuple[Processor, ProcessorResult]:
    if isinstance(processor, Extractor):
        return processor, processor.extract(frame)

    return processor, processor.feed_frame(frame)


@click.command()
@click.option('--path', type=click.STRING, required=True)
@click.option('--s3-access-key-id', type=click.STRING, required=True)
@click.option('--s3-secret-access-key', type=click.STRING, required=True)
@click.option('--input-bucket', type=click.STRING, required=True)
@click.option('--output-bucket', type=click.STRING, required=True)
def main(
    path: str,
    s3_access_key_id: str,
    s3_secret_access_key: str,
    input_bucket: str,
    output_bucket: str,
):
    s3_client = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net/',
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key,
    )
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': input_bucket, 'Key': path},
        ExpiresIn=3600 * 24,
    )
    extractors = [
        YExtractor(),
        SIExtractor(),
        TICalculator(),
        GLCMExtractor(),
        GLCMPropertyExtractor('correlation'),
        GLCMPropertyExtractor('contrast'),
        GLCMPropertyExtractor('energy'),
        GLCMPropertyExtractor('homogeneity'),
    ]
    feature_calculators = [
        STDCalculator('Y', 'CTI_std'),
        STDCalculator('SI'),
        STDCalculator('TI'),
        MeanCalculator('Y', 'CTI_mean'),
        MeanCalculator('SI'),
        MeanCalculator('TI'),
        MeanCalculator('GLCM_correlation'),
        MeanCalculator('GLCM_contrast'),
        MeanCalculator('GLCM_energy'),
        MeanCalculator('GLCM_homogeneity'),
        STDCalculator('GLCM_correlation'),
        STDCalculator('GLCM_contrast'),
        STDCalculator('GLCM_energy'),
        STDCalculator('GLCM_homogeneity'),
    ]
    processors = {
        processor.name(): processor
        for processor in chain(extractors, feature_calculators)
    }
    dependencies = {
        processor.name(): [processor.depends_on()] if processor.depends_on() else []
        for processor in chain(extractors, feature_calculators)
    }

    buffer = io.StringIO()
    fieldnames = [
        'width',
        'height',
        'format',
        'key_frame',
        'time',
        'pts',
        'dts',
    ]
    for calculator in feature_calculators:
        fieldnames.append(calculator.name())

    writer = csv.DictWriter(buffer, fieldnames=fieldnames, quoting=csv.QUOTE_STRINGS, delimiter='|')
    writer.writeheader()

    with Decoder(presigned_url) as decoder:
        with ThreadPoolExecutor() as pool:
            for frame in decoder:
                item = {
                    'width': frame.width,
                    'height': frame.height,
                    'format': frame.format.name,
                    'key_frame': int(frame.key_frame),
                    'time': frame.time,
                    'pts': frame.pts,
                    'dts': frame.dts,
                }
                extractors_results = {}
                frame_data = frame.to_ndarray()
                graph = TopologicalSorter(dependencies)
                graph.prepare()
                while graph.is_active():
                    ready_processors_names = graph.get_ready()
                    futures = []
                    for name in ready_processors_names:
                        processor = processors[name]
                        matrix = extractors_results.get(processor.depends_on(), frame_data)
                        future = pool.submit(run_processor, processor, matrix)
                        futures.append(future)

                    for future in as_completed(futures):
                        calc, result = future.result()
                        if isinstance(calc, Extractor):
                            extractors_results[calc.name()] = result
                        else:
                            item[calc.name()] = result
                    graph.done(*ready_processors_names)
                writer.writerow(item)

    buffer.seek(0)
    s3_client.put_object(
        Bucket=output_bucket,
        Key=f'{path}.csv',
        Body=buffer.getvalue().encode(),
        Metadata={'Content-Type': 'text/csv'}
    )


if __name__ == '__main__':
    load_dotenv()
    main(auto_envvar_prefix='CALCULATOR')
