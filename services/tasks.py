from celery import shared_task
from celery import Celery
from celery.signals import worker_process_init
import redis
import pickle

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

SHARED_DICT_KEY = "shared_stock_data"

@worker_process_init.connect
def setup_shared_dict(**kwargs):
    if not redis_client.exists(SHARED_DICT_KEY):
        initial_data = {}  # Initialize the dictionary
        redis_client.set(SHARED_DICT_KEY, pickle.dumps(initial_data))

@shared_task
def update_stock_data(stock_symbol, new_data):
    shared_dict = pickle.loads(redis_client.get(SHARED_DICT_KEY))

    shared_dict[stock_symbol] = new_data

    redis_client.set(SHARED_DICT_KEY, pickle.dumps(shared_dict))

    return shared_dict[stock_symbol]

@shared_task
def get_stock_data(stock_symbol):
    shared_dict = pickle.loads(redis_client.get(SHARED_DICT_KEY))

    return shared_dict.get(stock_symbol, "Stock data not found")
