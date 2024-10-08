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



# shared_dict = pickle.loads(redis_database.get("binance:ticker:data:REEFUSDT"))
#
#
# for time_interval, data in shared_dict.items():
#     print("Time interval: ", time_interval)
#     if time_interval == '1_min':
#         print("Data from reddis db: ", data.get('value'))
#     else:
#         print("Data from reddis db: ", data.get('values'))


time_interval =  [
      "2024-08-24T15:37:23.820000",
      "2024-08-24T16:39:50.655000",
      "2024-08-24T17:42:39.970000",
      "2024-08-24T18:45:31.113000",
      "2024-08-24T19:48:15.942000",
      "2024-08-24T20:50:43.411000",
      "2024-08-24T21:54:10.423000",
      "2024-08-24T22:57:28.201000",
      "2024-08-25T00:01:35.658000",
      "2024-08-25T01:05:42.715000",
      "2024-08-25T02:10:16.691000",
      "2024-08-25T03:15:03.875000",
      "2024-08-25T04:20:09.810000",
      "2024-08-25T05:25:37.514000",
      "2024-08-25T06:31:00.236000",
      "2024-08-25T07:36:41.253000",
      "2024-08-25T08:42:28.998000",
      "2024-08-25T09:48:56.009000",
      "2024-08-25T10:55:16.779000",
      "2024-08-25T12:01:40.105000",
      "2024-08-25T13:08:19.993000",
      "2024-08-25T14:15:45.008000",
      "2024-08-25T15:23:18.214000",
      "2024-08-25T16:31:37.121000",
      "2024-08-25T17:39:53.505000",
      "2024-08-25T18:48:28.742000",
      "2024-08-25T19:57:34.283000",
      "2024-08-25T21:06:40.298000",
      "2024-08-25T22:16:41.606000",
      "2024-08-25T23:26:41.111000",
      "2024-08-26T00:37:18.395000",
      "2024-08-26T01:47:51.806000",
      "2024-08-26T02:58:30.956000",
      "2024-08-26T04:09:42.195000",
      "2024-08-26T05:21:12.587000",
      "2024-08-26T06:33:14.886000",
      "2024-08-26T07:45:40.366000",
      "2024-08-26T08:58:24.994000",
      "2024-08-26T10:11:33.073000",
      "2024-08-26T11:25:00.648000",
      "2024-08-26T12:39:00.036000",
      "2024-08-26T13:53:52.231000",
      "2024-08-26T15:09:11.278000",
      "2024-08-26T16:24:35.918000",
      "2024-08-26T17:40:24.453000",
      "2024-08-26T18:56:17.839000",
      "2024-08-26T20:13:21.130000",
      "2024-08-26T21:30:06.530000",
      "2024-08-26T22:47:28.023000",
      "2024-08-27T00:05:43.416000",
      "2024-08-27T01:23:31.123000",
      "2024-08-27T02:43:03.805000",
      "2024-08-27T04:02:38.847000",
      "2024-08-27T05:22:41.078000",
      "2024-08-27T06:42:15.965000",
      "2024-08-27T08:01:51.829000",
      "2024-08-27T09:21:27.458000",
      "2024-08-27T10:41:03.837000",
      "2024-08-27T12:00:58.786000",
      "2024-08-27T13:20:50.826000",
      "2024-08-27T14:41:03.039000",
      "2024-08-27T16:01:39.119000",
      "2024-08-27T17:22:38.466000",
      "2024-08-27T18:43:43.916000",
      "2024-08-27T20:04:59.159000",
      "2024-08-27T21:26:06.963000",
      "2024-08-27T22:47:28.204000",
      "2024-08-28T00:09:15.781000",
      "2024-08-28T01:30:52.905000",
      "2024-08-28T02:52:37.193000",
      "2024-08-28T04:14:42.463000",
      "2024-08-28T05:36:39.043000"
    ]
volume_data =  [
      30173353285,
      29522672349,
      29627430651,
      29022561625,
      27852917516,
      26967863216,
      27776670311,
      28124077273,
      27349096019,
      27166755848,
      28583893104,
      29111613479,
      30305782692,
      30191616406,
      30377620214,
      32816415236,
      36023887148,
      37024882467,
      35966958176,
      35700363934,
      36382851193,
      37086909075,
      37154258047,
      38319463869,
      38257432357,
      37991686814,
      38393880428,
      38384615288,
      36971663956,
      37915779298,
      40732405443,
      41580883896,
      55445324787,
      56855067015,
      59619487581,
      60652965782,
      59482089565,
      59903037695,
      59407155069,
      60734848809,
      62444636944,
      64747983383,
      65530542544,
      65813323729,
      65135479247,
      65756684167,
      66098076959,
      65826322557,
      65300401445,
      62344039607,
      60742794262,
      48094883591,
      48640130840,
      46871858734,
      46679203057,
      43116817563,
      42449151113,
      41640013483,
      39175100090,
      34852615207,
      33010923900,
      31934076020,
      31016394402,
      29488333722,
      29327159145,
      29835309161,
      34145082956,
      34975933527,
      35259476121,
      32354479371,
      28993891084,
      28269952896
]


print(len(time_interval))
print(len(volume_data))

