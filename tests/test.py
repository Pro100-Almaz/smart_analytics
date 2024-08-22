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

for i in range(1000):
    volume_response = requests.get('https://fapi.binance.com/fapi/v1/fundingRate')
    if volume_response.status_code == 200:
        volume_data = volume_response.json()
        print(volume_data[0])
        print(datetime.datetime.fromtimestamp(volume_data[0]["fundingTime"] / 1000.0))
        sleep(60)