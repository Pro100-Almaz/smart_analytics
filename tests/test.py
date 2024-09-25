# from collections import deque
#
# test_dict = {
#     "val": {
#         "m_v1": 1,
#         "m_v2": 2,
#         "m_v3": 3,
#         "m_v4": 4,
#     },
#     "window": deque(maxlen=5)
# }
#
# window = test_dict.get("window")
#
# for i in range(15):
#     window.append(i)
#
#     print(window)
#     print(min(window))
#     print(window.index(min(window)))
#
# test_window = window
# test_window.append(19)
# print(test_dict)
import requests
from time import sleep
import datetime

# for i in range(1000):
#     volume_response = requests.get('https://fapi.binance.com/fapi/v1/fundingRate')
#     if volume_response.status_code == 200:
#         volume_data = volume_response.json()
#         print(volume_data[0])
#         print(datetime.datetime.fromtimestamp(volume_data[0]["fundingTime"] / 1000.0))
#         sleep(60)

# from celery import Celery
#
# app = Celery('stock_updater')
#
# # Purge all tasks from the specified queue
# app.control.purge()
import redis
import pickle
import statistics

REDIS_HOST = "localhost"
REDIS_PORT = 12228
REDIS_DB = 0

redis_database = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# main_data = requests.get('https://fapi.binance.com/fapi/v1/ticker/24hr')
#
# if main_data.status_code == 200:
#     main_data = main_data.json()
#     for ticker in main_data:
#         if 'closeTime' in ticker and ticker['closeTime'] is not None:
#             try:
#                 ticker['openPositionDay'] = datetime.fromtimestamp(ticker['closeTime'] / 1000).strftime(
#                     '%d-%m-%Y | %H')
#             except (ValueError, TypeError) as e:
#                 print(f"Error processing closeTime: {e}")
#                 ticker['openPositionDay'] = None
#         else:
#             print("closeTime not found or invalid in ticker")
#             ticker['openPositionDay'] = None
#
#     current_date = statistics.mode([ticker['openPositionDay'] for ticker in main_data])
#     not_usdt_symbols = [ticker['symbol'] for ticker in main_data if 'USDT' not in ticker['symbol']]
#     delete_symbols = [ticker['symbol'] for ticker in main_data if ticker['openPositionDay'] != current_date]
#
#     exchange_info_data = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo').json()['symbols']
#     not_perpetual_symbols = [info['symbol'] for info in exchange_info_data if info['contractType'] != 'PERPETUAL']
#     full_symbol_list_to_delete = set(not_usdt_symbols + delete_symbols + not_perpetual_symbols)
#     volume_data = [ticker for ticker in main_data if ticker['symbol'] not in full_symbol_list_to_delete]
#
#     sorted_data_volume = sorted(volume_data, key=lambda x: float(x['quoteVolume']))



shared_dict = pickle.loads(redis_database.get("binance:ticker:data:REEFUSDT"))


for time_interval, data in shared_dict.items():
    print("Time interval: ", time_interval)
    if time_interval == '1_min':
        print("Data from reddis db: ", data.get('value'))
    else:
        print("Data from reddis db: ", data.get('values'))


