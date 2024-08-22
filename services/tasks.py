import os

import requests
from dotenv import load_dotenv

from celery import shared_task
from celery_app import Celery
from celery.signals import worker_process_init
import pickle
from collections import deque

from database import database, redis_database as redis_client

load_dotenv()

app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

SHARED_DICT_KEY = "binance:ticker:data"
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')


@shared_task
def push_stock_data(stock_symbol, new_data: float):
    stock_key = f"{SHARED_DICT_KEY}:{stock_symbol}"

    if not redis_client.exists(stock_key):
        shared_dict = {
            "1_min": {
                "value": None,
                "diff": []
            },
            "5_min": {
                "values" : deque(maxlen=5),
                "min": None,
                "max": None,
                "diff": []
            },
            "15_min": {
                "values": deque(maxlen=15),
                "min": None,
                "max": None,
                "diff": []
            },
            "60_min": {
                "values": deque(maxlen=60),
                "min": None,
                "max": None,
                "diff": []
            },
        }
    else:
        shared_dict = pickle.loads(redis_client.get(stock_key))

    for interval_type, current_data in shared_dict.items():
        if interval_type == "1_min":
            if current_data.get("value"):
                old_data = current_data.get("value", 0.001)
                current_data["diff"] = [old_data - new_data,
                                        round((old_data - new_data) / abs(old_data) * 100, 3)]
            else:
                current_data["diff"] = [new_data, 0]

            current_data["value"] = float(new_data)
        else:
            sliding_window = current_data.get("values")
            sliding_window.append(new_data)

            min_value = min(sliding_window)
            max_value = max(sliding_window)

            current_data["diff"] = [min_value - max_value, round((min_value - max_value) / abs(min_value) * 100, 3)]
            current_data["min"] = min_value
            current_data["max"] = max_value

    redis_client.set(stock_key, pickle.dumps(shared_dict))
    redis_client.expire(stock_key, 3600)


@shared_task
def update_stock_data(stock_symbol, new_data: float):
    stock_key = f"{SHARED_DICT_KEY}:{stock_symbol}"
    shared_dict = pickle.loads(redis_client.get(stock_key))

    for interval_type, current_data in shared_dict.items():
        if interval_type == "1_min":
            if current_data.get("value"):
                old_data = current_data.get("value", 0.001)
                current_data["diff"] = [old_data - new_data,
                                        round((old_data - new_data) / abs(old_data) * 100, 3)]
            else:
                current_data["diff"] = [new_data, 0]
        else:
            sliding_window = current_data.get("values")
            sliding_window[-1] = new_data

            min_value = min(sliding_window)
            max_value = max(sliding_window)

            current_data["diff"] = [min_value - max_value, round((min_value - max_value) / abs(min_value) * 100, 3)]
            current_data["min"] = min_value
            current_data["max"] = max_value

    redis_client.set(stock_key, pickle.dumps(shared_dict))
    redis_client.expire(stock_key, 3600)


@shared_task
def notify_by_telegram(active_name, percent, user_id):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": user_id,
        "text": f"{active_name} -> {percent}"
    }

    if user_id == 123:
        reply_markup = {
            "inline_keyboard": [[{
                "text": "Lets trade!",
                "web_app": {"url": "https://smart-trade-kappa.vercel.app/"}
            }]]
        }

        payload["reply_markup"] = reply_markup

    response = requests.post(url, json=payload)

    if response.status_code == 200:
        pass
    else:
        database.execute(
            """
                
            """
        )
