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


def last_impulse_notification(time_interval: int):
    database.connect()

    users = database.execute_with_return(
        """
            SELECT *
            FROM users.user_notification
            WHERE notification_type = 'last_impulse' AND interval = %s;
        """, (time_interval,)
    )

    for user in users:
        interval, percent = user[6].split(":")




last_impulse_notification(15)