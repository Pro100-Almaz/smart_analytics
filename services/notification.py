import requests
import os

import schedule
import logging
import pickle
from logging.handlers import RotatingFileHandler

from database import database, redis_database
from tasks import notify_by_telegram


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
    database.connect()

    prefix = "binance:ticker:data:"

    cursor = 0
    matching_keys = []
    current_data = {}

    while True:
        cursor, keys = redis_database.scan(cursor=cursor, match=f'{prefix}*')
        matching_keys.extend(keys)

        if cursor == 0:
            break

    matching_keys = [key.decode("utf-8") for key in matching_keys]
    for key in matching_keys:
        current_data[key] = pickle.loads(redis_database.get(key))

    users = database.execute_with_return(
        """
            SELECT user_id, condition
            FROM users.user_notification
            WHERE notification_type = 'last_impulse' AND active = true;
        """
    )


    for user in users:
        user_interval, user_percent = user[1].split(":")
        user_percent = float(user_percent)

        for data_active, data_intervals in current_data.items():
            temp_data = data_intervals.get(user_interval, None)

            if temp_data and abs(temp_data.get('diff', {})[1]) >= user_percent:
                print("Found Percent!")
                notify_by_telegram.delay(data_active, temp_data.get('diff', {})[1], user[0])



last_impulse_notification()