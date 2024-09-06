import os

import requests
import pickle
from dotenv import load_dotenv

from database import database, redis_database


load_dotenv()

SHARED_DICT_KEY = "binance:ticker:data"
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')

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
            SELECT un.user_id, condition, id
            FROM users.user_notification un
            JOIN users.notification_settings ns ON un.user_id = ns.user_id AND ns.last_impulse
            WHERE notification_type = 'last_impulse' AND active = true;
        """
    )

    for user in users:
        user_interval, user_percent = user[1].split(":")
        user_percent = float(user_percent)

        for data_active, data_intervals in current_data.items():
            data_intervals = dict(data_intervals)
            temp_data = data_intervals.get(user_interval, None)

            try:
                min_diff = temp_data.get('diff', [])[0] if abs(temp_data.get('diff', [])[0]) >= user_percent else False
                max_diff = temp_data.get('diff', [])[1] if abs(temp_data.get('diff', [])[1]) >= user_percent else False
            except Exception as e:
                continue

            if temp_data and (min_diff or max_diff):
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

                if is_it_sent:
                    continue

                percent = min_diff if min_diff != False else max_diff

                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

                if user_interval == "1_min":
                    time_text = "(за последние 1 мин)"
                elif user_interval == "5_min":
                    time_text = "(за последние 5 мин)"
                elif user_interval == "15_min":
                    time_text = "(за последние 15 мин)"
                else:
                    time_text = "(за последние 60 мин)"

                if percent > 0:
                    text_for_notification = "\n".join(["🔔❗️Обратите внимание❗️🔔",
                                            f"Торговая пара {active_name} дала импульс цены в {percent}% {time_text} 🟢📈"])
                else:
                    text_for_notification = "\n".join(["🔔❗️Обратите внимание❗️🔔",
                                            f"Торговая пара {active_name} дала импульс цены в {percent}% {time_text} 🔴📉"])

                payload = {
                    "chat_id": telegram_id,
                    "text": text_for_notification
                }

                response = requests.post(url, json=payload)
                try:
                    day_before_price = database.execute_with_return(
                        """
                        SELECT close_price
                        FROM data_history.funding t1
                        JOIN data_history.kline_1 t2 ON t1.stock_id = t2.stock_id
                        WHERE t1.symbol = %s
                        ORDER BY t2.open_time DESC 
                        LIMIT 1 OFFSET 1439;
                        """, (active_name,)
                    )
                except Exception as e:
                    print("Error arose while making query of day_before_price: ", e)
                    continue

                print("Making the notification")
                current_price = temp_data.get('values', [])[-1]

                if day_before_price:
                    day_percent = round(((current_price - day_before_price[0][0]) / day_before_price[0][0]) * 100, 2)
                else:
                    day_percent = 0

                try:
                    database.execute(
                        """
                            INSERT INTO users.notification (type, date, text, status, active_name, telegram_id, percent, day_percent)
                            VALUES (%s, current_timestamp, %s, %s, %s, %s, $s, $s);
                        """, (user[2], response.text, response.ok, active_name, telegram_id, percent, day_percent)
                    )
                except Exception as e:
                    print("Error arose while saving data into users.notification: ", e)

    database.disconnect()
