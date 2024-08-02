import logging
import sys
import os
from logtail import LogtailHandler

from dotenv import load_dotenv

load_dotenv()

token = os.getenv('LOG_TOKEN')
logger = logging.getLogger()

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler('app.log')
better_stack_handler = LogtailHandler(source_token=token)

stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.setLevel(logging.INFO)
logger.handlers = [better_stack_handler, stream_handler]