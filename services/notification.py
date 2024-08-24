import os

import requests
import pickle
from dotenv import load_dotenv

from database import database, redis_database


load_dotenv()

SHARED_DICT_KEY = "binance:ticker:data"
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

def last_impulse_notification():
    print("I am in notification mode!")
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
            SELECT user_id, condition, id
            FROM users.user_notification
            WHERE notification_type = 'last_impulse' AND active = true;
        """
    )

    for user in users:
        user_interval, user_percent = user[1].split(":")
        user_percent = float(user_percent)

        for data_active, data_intervals in current_data.items():
            data_intervals = dict(data_intervals)
            temp_data = data_intervals.get(user_interval, None)

            if temp_data and abs(temp_data.get('diff', {})[1]) >= user_percent:
                print("Find percent")
                telegram_id = database.execute_with_return(
                    """
                        SELECT telegram_id
                        FROM users."user"
                        WHERE user_id = %s;
                    """, (user[0], )
                )

                active_name = (data_active.split(":"))[-1]
                telegram_id = telegram_id[0][0]

                is_it_sent = database.execute_with_return(
                    """
                        SELECT 
                            date,
                            NOW() AS current_time,
                            (NOW() - INTERVAL '1 hour') < date AS is_less_than_one_hour
                        FROM 
                            users.notification
                        WHERE 
                            active_name = %s AND telegram_id = %s
                        ORDER BY 
                            date DESC
                        LIMIT 1;
                    """, (active_name, telegram_id)
                )

                if is_it_sent[0][0]:
                    return "already_sent"

                percent = temp_data.get('diff', {})[1]

                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

                payload = {
                    "chat_id": telegram_id,
                    "text": "\n".join(["🔔❗️Обратите внимание❗️🔔",
                                       f"Торговая пара {active_name} дала импульс цены в {percent}% 🔴📈"])
                }

                response = requests.post(url, json=payload)

                database.execute(
                    """
                        INSERT INTO users.notification (type, date, text, status, active_name, telegram_id)
                        VALUES (%s, current_timestamp, %s, %s, %s, %s);
                    """, (user[0], response.text, response.ok, active_name, telegram_id)
                )

                return f"send_notify to user: {user[0]}"

    database.disconnect()
