from urllib.parse import urlencode

from celery import Celery

from settings import Settings


def configure_celery(settings: Settings):
    app = Celery('encoder_tasks')

    query_params = {}
    if settings.DATABASE_SSL:
        query_params['sslmode'] = 'require'
    query_params = urlencode(query_params)
    db_string = settings.DATABASE_URL
    if query_params:
        db_string += f'?{query_params}'

    app.conf.update(
        broker_url=settings.CELERY_BROKER_URL,
        result_backend=f'db+{db_string}',
        s3_endpoint_url=settings.S3_ENDPOINT_URL,
        s3_access_key_id=settings.AWS_ACCESS_KEY_ID,
        s3_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        s3_bucket=settings.S3_BUCKET,
        database_url=db_string,
        task_default_queue=settings.CELERY_QUEUE_NAME,
        regressor_path=settings.REGRESSOR_PATH,
    )
    app.conf.worker_prefetch_multiplier = settings.CELERY_PREFETCH_MULTIPLIER
    # Special for SQS
    app.conf.broker_transport_options = {'is_secure': True}

    return app
