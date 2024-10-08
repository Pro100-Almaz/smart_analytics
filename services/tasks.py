import os

from dotenv import load_dotenv

from celery_app import app
import pickle
from collections import deque

from database import redis_database as redis_client

load_dotenv()

SHARED_DICT_KEY = "binance:ticker:data"
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')


# @worker_process_init.connect
# def connect_database(**kwargs):
#     try:
#         database.connect()
#         print("Database connected successfully.")
#     except Exception as e:
#         print(f"Error connecting to database: {e}")
#
#
# @worker_shutdown.connect
# def close_database_connection(**kwargs):
#     try:
#         database.disconnect()
#         print("Database connection closed successfully.")
#     except Exception as e:
#         print(f"Error closing database connection: {e}")

# def send_to_rabbitmq(message):
#     # Establish a connection with RabbitMQ
#     connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
#     channel = connection.channel()
#
#     # Declare the queue to ensure it exists
#     channel.queue_declare(queue='telegram_queue')
#
#     # Publish the message to the queue
#     channel.basic_publish(
#         exchange='',
#         routing_key='telegram_queue',
#         body=json.dumps(message)
#     )
#
#     print(f" [x] Sent {message}")
#
#     # Close the connection
#     connection.close()

@app.task(ignore_result=True)
def push_stock_data(stock_symbol, new_data: float):
    stock_key = f"{SHARED_DICT_KEY}:{stock_symbol}"

    if not redis_client.exists(stock_key):
        shared_dict = {
            "1_min": {
                "value": new_data,
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
                current_data["diff"] = [round(100 - (old_data * 100 / new_data), 2)]
            else:
                current_data["diff"] = [0]

            current_data["value"] = new_data
        else:
            sliding_window = current_data.get("values")
            sliding_window.append(new_data)

            if current_data.get("min") and current_data.get("max"):
                # current_data["diff"] = [
                #     round(((new_data - current_data.get("min", 0)) / abs(current_data.get("min", 0)) * 100), 2),
                #     round(((new_data - current_data.get("max", 0)) / abs(current_data.get("max", 0)) * 100), 2)
                # ]

                current_data["diff"] = [
                    round(100 - (current_data.get("min", 0) * 100 / new_data), 2),
                    round(100 - (current_data.get("max", 0) * 100 / new_data), 2),
                ]

            min_value = min(sliding_window)
            max_value = max(sliding_window)
            current_data["min"] = min_value
            current_data["max"] = max_value

    redis_client.set(stock_key, pickle.dumps(shared_dict))
    redis_client.expire(stock_key, 3600)

    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match='celery-task-meta*', count=10000)
        if keys:
            redis_client.delete(*keys)
        if cursor == 0:
            break


@app.task
def update_stock_data(stock_symbol, new_data: float):
    stock_key = f"{SHARED_DICT_KEY}:{stock_symbol}"
    try:
        shared_dict = pickle.loads(redis_client.get(stock_key))
    except:
        return "create_stock_key"

    for interval_type, current_data in shared_dict.items():
        if interval_type == "1_min":
            if current_data.get("value"):
                old_data = current_data.get("value", 0.001)
                current_data["diff"] = [round(100 - (old_data * 100 / new_data), 2)]
            else:
                current_data["diff"] = [0]
        else:
            sliding_window = current_data.get("values")
            sliding_window[-1] = new_data

            # current_data["diff"] = [
            #     round(((new_data - current_data.get("min", 0)) / abs(current_data.get("min", 0)) * 100), 2),
            #     round(((new_data - current_data.get("max", 0)) / abs(current_data.get("max", 0)) * 100), 2)
            # ]
            current_data["diff"] = [
                round(100 - (current_data.get("min", 0) * 100 / new_data), 2),
                round(100 - (current_data.get("max", 0) * 100 / new_data), 2),
            ]

            min_value = min(sliding_window)
            max_value = max(sliding_window)
            current_data["min"] = min_value
            current_data["max"] = max_value

    redis_client.set(stock_key, pickle.dumps(shared_dict))
    redis_client.expire(stock_key, 3600)
