import os
from time import sleep

import requests
import logging

from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

from tasks import update_stock_data, push_stock_data
from notification import last_impulse_notification
from utils import save_http_data


load_dotenv()

log_directory = "logs"
log_filename = "http_candlestick_receiver.log"
log_file_path = os.path.join(log_directory, log_filename)

if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = RotatingFileHandler(log_file_path, maxBytes=200000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

load_dotenv()

proxies_string = os.getenv('PROXIES')

proxy_list = proxies_string.split(',')

proxies_string = os.getenv('PROXIES')
proxy = proxies_string.split(',')[0]

proxies = {
    "http": proxy
}


# def unix_to_date(unix):
#     timestamp_in_seconds = unix / 1000
#
#     utc_time = datetime.fromtimestamp(timestamp_in_seconds, tz=timezone.utc)
#
#     adjusted_time = utc_time + timedelta(hours=5)
#
#     date = adjusted_time.strftime('%M')
#     return date


def get_data():
    url = 'https://fapi.binance.com/fapi/v1/ticker/24hr'
    response = requests.get(url, proxies=proxies)
    main_data = response.json()
    frequent_date = {}
    result_list = []

    for record in main_data:
        record['openPositionDay'] = datetime.fromtimestamp(record['closeTime'] / 1000).strftime('%d-%m-%Y')
        if record['openPositionDay'] not in frequent_date:
            frequent_date[record['openPositionDay']] = 1
        else:
            frequent_date[record['openPositionDay']] += 1

    current_date = max(frequent_date, key=frequent_date.get)

    not_usdt_symbols = [record['symbol'] for record in main_data if 'USDT' not in record['symbol']]

    delete_symbols = []
    for record in main_data:
        if record['openPositionDay'] != current_date:
            delete_symbols.append(record['symbol'])

    exchange_info_data = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo').json()
    exchange_info_data = exchange_info_data.get('symbols')

    not_perpetual_symbols = [record['symbol'] for record in exchange_info_data if
                             record['contractType'] != 'PERPETUAL']

    full_symbol_list_to_delete = set(not_usdt_symbols + delete_symbols + not_perpetual_symbols)

    for i in range(len(main_data)):
        if main_data[i]['symbol'] not in full_symbol_list_to_delete:
            result_list.append(main_data[i])

    return result_list


def candlestick_receiver():
    phase_minute = None
    push_new_value = False
    iteration_value = 1

    while True:
        logger.info(f"Started {iteration_value} iteration!")
        data = get_data()
        current_time = datetime.now().minute
        push_new_value = True if current_time != phase_minute else False
        phase_minute = current_time

        for record in data:
            # unix_to_date(record.get('openTime'))
            active_name = record.get('symbol')
            last_value = float(record.get('lastPrice', {}))

            try:
                if push_new_value:
                    logger.info(f"Push stock data new minute value: {current_time}")
                    push_stock_data.delay(active_name, last_value)
                    save_http_data(record)

                else:
                    update_stock_data.delay(active_name, last_value)
            except Exception as e:
                logger.error(f"An error occurred while processing data, at proxy {proxy}: {e}")
                break

            try:
                last_impulse_notification()
            except Exception as e:
                logger.error("Error while sending notification: ", e)

        logger.info(f"Ended {iteration_value} iteration!")
        iteration_value += 1


# schedule.every(5).seconds.do(main_runner)
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)

if "__main__" == __name__:
    candlestick_receiver()

