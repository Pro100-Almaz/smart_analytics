from celery import shared_task
from celery_app import Celery
from celery.signals import worker_process_init
import redis
import pickle
from collections import deque

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

SHARED_DICT_KEY = "binance:ticker:data"

@shared_task
def update_stock_data(stock_symbol, new_data: float):
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

    return shared_dict[stock_symbol]

@shared_task
def get_stock_data():
    shared_dict = pickle.loads(redis_client.get(f"{SHARED_DICT_KEY}:*"))
    print(shared_dict)

    return shared_dict or None
