from celery import Celery

from settings import Settings


def configure_celery(settings: Settings):
    app = Celery('encoder_tasks')

    app.conf.update(
        broker_url=settings.CELERY_BROKER_URL,
        result_backend=f'db+{settings.DATABASE_URL}',
        s3_endpoint_url=settings.S3_ENDPOINT_URL,
        s3_access_key_id=settings.AWS_ACCESS_KEY_ID,
        s3_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        s3_bucket=settings.S3_BUCKET,
        database_url=settings.DATABASE_URL,
        task_default_queue=settings.CELERY_QUEUE_NAME,
    )
    # Special for SQS
    app.conf.broker_transport_options = {'is_secure': True}

    return app
