import json
from decimal import Decimal, ROUND_DOWN

import requests
import datetime
import os
import time

import schedule
import logging
from logging.handlers import RotatingFileHandler

from database import database, redis_database
from tasks import get_stock_data


log_directory = "logs"
log_filename = "notification.log"
log_file_path = os.path.join(log_directory, log_filename)

if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(log_file_path, maxBytes=2000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def last_impulse_notification():
    stock_data = get_stock_data.delay()
    database.connect()

    users = database.execute_with_return(
        """
            SELECT user_id, condition
            FROM users.user_notification
            WHERE notification_type = 'last_impulse' AND active = true;
        """
    )
    try:
        data = stock_data.get()
    except Exception as e:
        print(f"Task failed or timed out: {e}")
        logger.error(f"Task failed or timed out, by error: {e}")

    for user in users:
        print(user)




last_impulse_notification()