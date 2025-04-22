import logging
import sys

from dotenv import load_dotenv

from settings import get_settings
from tasks import TranscodeVideoTask, configure_celery


def configure_logging():
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.INFO)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s\n',
        handlers=[handler],
        force=True,
    )


configure_logging()
load_dotenv()
celery = configure_celery(get_settings())
transcode_video_task = celery.register_task(TranscodeVideoTask)
