import logging
import os
import statistics
import time

from datetime import datetime
from logging.handlers import RotatingFileHandler

import requests
import schedule

from database import database, redis_database
from notification import ticker_tracking_notification

log_directory = "logs"
log_filename = "funding_rate.log"
log_file_path = os.path.join(log_directory, log_filename)

if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

handler = RotatingFileHandler(log_file_path, maxBytes=2000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


except_list = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "BTCDOMUSDT"]


def get_volume_data():
    main_data = requests.get('https://fapi.binance.com/fapi/v1/ticker/24hr')

    if main_data.status_code == 200:
        main_data = main_data.json()
        for ticker in main_data:
            if 'closeTime' in ticker and ticker['closeTime'] is not None:
                try:
                    ticker['openPositionDay'] = datetime.fromtimestamp(ticker['closeTime'] / 1000).strftime(
                        '%d-%m-%Y | %H')
                except (ValueError, TypeError) as e:
                    print(f"Error processing closeTime: {e}")
                    ticker['openPositionDay'] = None
            else:
                print("closeTime not found or invalid in ticker")
                ticker['openPositionDay'] = None

        current_date = statistics.mode([ticker['openPositionDay'] for ticker in main_data])
        not_usdt_symbols = [ticker['symbol'] for ticker in main_data if 'USDT' not in ticker['symbol']]
        delete_symbols = [ticker['symbol'] for ticker in main_data if ticker['openPositionDay'] != current_date]

        exchange_info_data = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo').json()['symbols']
        not_perpetual_symbols = [info['symbol'] for info in exchange_info_data if info['contractType'] != 'PERPETUAL']
        full_symbol_list_to_delete = set(not_usdt_symbols + delete_symbols + not_perpetual_symbols)
        volume_data = [ticker for ticker in main_data if ticker['symbol'] not in full_symbol_list_to_delete]

        updated_volume_data = [d for d in volume_data if d['symbol'] not in except_list]

        return sorted(updated_volume_data, key=lambda x: float(x['priceChangePercent']))


def get_symbols():
    main_data = requests.get('https://fapi.binance.com/fapi/v1/ticker/24hr').json()

    for ticker in main_data:
        if 'closeTime' in ticker and ticker['closeTime'] is not None:
            try:
                ticker['openPositionDay'] = datetime.fromtimestamp(ticker['closeTime'] / 1000).strftime(
                    '%d-%m-%Y | %H')
            except (ValueError, TypeError) as e:
                print(f"Error processing closeTime: {e}")
                ticker['openPositionDay'] = None
        else:
            print("closeTime not found or invalid in ticker")
            ticker['openPositionDay'] = None

    current_date = statistics.mode([ticker['openPositionDay'] for ticker in main_data])
    not_usdt_symbols = [ticker['symbol'] for ticker in main_data if 'USDT' not in ticker['symbol']]
    delete_symbols = [ticker['symbol'] for ticker in main_data if ticker['openPositionDay'] != current_date]
    exchange_info_data = requests.get('https://fapi.binance.com/fapi/v1/exchangeInfo').json()['symbols']
    not_perpetual_symbols = [info['symbol'] for info in exchange_info_data if info['contractType'] != 'PERPETUAL']
    full_symbol_list_to_delete = set(not_usdt_symbols + delete_symbols + not_perpetual_symbols)
    main_data = [ticker for ticker in main_data if ticker['symbol'] not in full_symbol_list_to_delete]
    ticker_list = sorted([ticker['symbol'] for ticker in main_data])
    return ticker_list


def get_funding_data():
    funding_response = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex")
    if funding_response.status_code == 200:
        funding_data = funding_response.json() if funding_response.status_code == 200 else None

        tickers = get_symbols()

        funding_rate_list = [
            {
                'symbol': value['symbol'],
                'lastFundingRate': round(float(value['lastFundingRate']) * 100, 4),
                'markPrice': float(value['markPrice']),
                'time': value['time']
            } for value in funding_data if value['symbol'] in tickers]

        sorted_data_funding = sorted(funding_rate_list, key=lambda x: float(x['lastFundingRate']))

        return sorted_data_funding


def main_runner():
    try:
        database.connect()
        logger.info("I am in main_runner script")
        tt_users = database.execute_with_return(
            """
                WITH un AS (
                    SELECT id, user_id, condition, 
                           NOW() - make_interval(mins := split_part(condition, '_', 1)::INTEGER) AS time_interval
                    FROM users.user_notification
                    WHERE notification_type = 'ticker_tracking' AND active = true
                )
                SELECT
                    COALESCE(users.notification.telegram_id, NULL) AS telegram_id,
                    un.user_id as user_id,
                    un.condition as condition,
                    un.id as type
                FROM un
                LEFT JOIN users.notification ON users.notification.type = un.id
                WHERE (users.notification.date <= un.time_interval OR users.notification.date IS NULL)
                ORDER BY users.notification.date DESC NULLS LAST
                LIMIT 1;
            """
        )

        print("ticker tracking listed users: ", tt_users)

        funding_data = get_funding_data()
        volume_data = get_volume_data()

        print("run the data collection!")

        if volume_data == "Error with DB":
            logging.error("Error with DB")
            return

        notify_list = {}

        for tt_user in tt_users:
            time_interval, ticker_name = tt_user[2].split(":")
            user_telegram_id = tt_user[0]

            if not tt_user[0]:
                telegram_id = database.execute_with_return(
                    """
                        SELECT telegram_id
                        FROM users.user
                        WHERE user_id = %s;
                    """, (tt_user[1],)
                )

                user_telegram_id = telegram_id[0][0]

            if ticker_name not in notify_list.keys():
                notify_list[ticker_name] = {
                    'type': tt_user[3],
                    'telegram_id': [user_telegram_id]
                }
            else:
                notify_list[ticker_name]['telegram_id'].append(user_telegram_id)

        print("First step of collecting notify list, the value is: ", notify_list)

        for index, record in enumerate(volume_data):
            if record.get('symbol', None) in notify_list.keys():
                symbol_value = record.get('symbol')

                volume_data_15_min = database.execute_with_return(
                    """
                        WITH fd AS (
                            SELECT stock_id
                            FROM data_history.funding
                            WHERE symbol = %s
                        )
                        SELECT vd.last_price, vd.quote_volume
                        FROM data_history.volume_data vd
                        JOIN fd ON vd.stock_id = fd.stock_id
                        ORDER BY vd.open_time DESC
                        LIMIT 1 OFFSET 14;
                    """, (symbol_value,)
                )

                notify_list[symbol_value].update({
                    'current_price': record.get('lastPrice', 0),
                    'price_change': round((float(volume_data_15_min[0][0]) * 100 / float(record.get('lastPrice', 1))) - 100, 2),
                    'current_volume': record.get('quoteVolume', 0),
                    'volume_change': round((float(volume_data_15_min[0][1]) * 100 / float(record.get('quoteVolume', 1))) - 100, 2),
                    'top_place': index+1
                })

        print("Second step of collecting notify list, the value is: ", notify_list)

        for index, record in enumerate(funding_data):
            if record.get('symbol', None) in notify_list.keys():
                symbol_value = record.get('symbol')

                funding_data_15_min = database.execute_with_return(
                    """
                        WITH fd AS (
                            SELECT stock_id
                            FROM data_history.funding
                            WHERE symbol = %s
                        )
                        SELECT vd.last_price, vd.quote_volume
                        FROM data_history.funding_data vd
                        JOIN fd ON vd.stock_id = fd.stock_id
                        ORDER BY vd.open_time DESC
                        LIMIT 1 OFFSET 14;
                    """, (symbol_value,)
                )

                notify_list[symbol_value].update({
                    'current_funding_rate': record.get('lastFundingRate', 0),
                    'funding_rate_change': round((float(funding_data_15_min[0][0]) * 100 / float(record.get('lastFundingRate', 1))) - 100, 2)
                })

        print("Third step of collecting notify list, the value is: ", notify_list)

        if notify_list:
            try:
                ticker_tracking_notification(notify_list)
            except Exception as e:
                print("Exception occurred in ticker tracking notification, error message: ", e)
    except Exception as e:
        logging.error(f"Error in main_runner: {e}")
    finally:
        database.disconnect()


schedule.every(60).seconds.do(main_runner)

while True:
    schedule.run_pending()
    time.sleep(1)

