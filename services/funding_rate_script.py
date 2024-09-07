import json
from decimal import Decimal

import requests
from datetime import datetime, timezone
import os
import time
import statistics

import schedule
import logging
from logging.handlers import RotatingFileHandler

from database import database, redis_database


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
        database.connect()

        main_data = main_data.json()
        for ticker in main_data:
            if 'closeTime' in ticker and ticker['closeTime'] is not None:
                try:
                    ticker['openPositionDay'] = datetime.fromtimestamp(ticker['closeTime'] / 1000).strftime(
                        '%d-%m-%Y')
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

        sorted_data_volume = sorted(updated_volume_data, key=lambda x: float(x['quoteVolume']))
        now = datetime.now()

        try:
            rounded_time = now.replace(second=0, microsecond=0)

            first_5 = sorted_data_volume[-5:]

            for record in first_5:
                stock_id = database.execute_with_return(
                    """
                        SELECT stock_id
                        FROM data_history.funding
                        WHERE symbol = %s 
                    """, (record['symbol'],),
                )

                if not stock_id:
                    continue

                stock_id = stock_id[0][0]

                quote_volume_5m = database.execute_with_return(
                    """
                    SELECT quote_volume
                    FROM data_history.volume_data
                    WHERE stock_id = %s
                    ORDER BY open_time DESC
                    LIMIT 1 OFFSET 4;
                    """, (stock_id,)
                )

                if quote_volume_5m:
                    record['5_min_value'] = float(quote_volume_5m[0][0])


            redis_data = {
                'last_update_time': rounded_time.isoformat(),
                'first_5': first_5
            }

            json_data = json.dumps(redis_data)
            redis_database.set('funding:top:5:tickets:volume', json_data)
        except Exception as e:
            logging.error(f"Error arose while trying to insert top tickets by volume into Reddis, error message:{e}")

        for record in updated_volume_data:
            try:
                stock_id = database.execute_with_return(
                    """
                        WITH ins AS (
                            INSERT INTO data_history.funding (symbol, company_name)
                            VALUES (%s, %s)
                            ON CONFLICT (symbol) DO NOTHING
                            RETURNING stock_id
                        )
                        SELECT stock_id FROM ins
                        UNION ALL
                        SELECT stock_id FROM data_history.funding
                        WHERE symbol = %s AND NOT EXISTS (SELECT 1 FROM ins)
                    """, (record["symbol"], "crypto", record["symbol"])
                )
            except Exception as e:
                logging.error(f"Error arose while trying to insert funding names into DB, error message:{e}")
                return "Error with DB"

            stock_id = stock_id[0][0]

            open_time = datetime.fromtimestamp(record["openTime"] / 1000.0)
            close_time = datetime.fromtimestamp(record["closeTime"] / 1000.0)

            try:
                records_count = database.execute_with_return(
                    """
                        SELECT COUNT(*) FROM data_history.volume_data WHERE stock_id = %s;
                    """, (stock_id,)
                )

                if records_count[0][0] >= 43200:
                    database.execute("""
                                        DELETE FROM data_history.volume_data
                                        WHERE stock_id = (
                                            SELECT stock_id FROM data_history.volume_data
                                            WHERE stock_id = %s
                                            ORDER BY open_time ASC
                                            LIMIT 1
                                        );
                        """, (stock_id,))

                    print(f"Deleted the oldest record with column value '{stock_id}'")

                database.execute(
                    """
                        INSERT INTO data_history.volume_data (stock_id, price_change, price_change_percent, 
                        weighted_avg_price, last_price, last_qty, open_price, high_price, volume, quote_volume, 
                        open_time, close_time, first_id, last_id, count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (stock_id, Decimal(record["priceChange"]), Decimal(record["priceChangePercent"]),
                          Decimal(record["weightedAvgPrice"]), Decimal(record["lastPrice"]), Decimal(record["lastQty"]),
                          Decimal(record["openPrice"]), Decimal(record["highPrice"]), Decimal(record["volume"]),
                          Decimal(record["quoteVolume"]), open_time, close_time, record["firstId"], record["lastId"],
                          record["count"])
                )
            except Exception as e:
                logging.error(f"Error arose while trying to insert volume data into DB, error message:{e}")
                return "Error with DB"

        database.disconnect()


def get_symbols():
    main_data = requests.get('https://fapi.binance.com/fapi/v1/ticker/24hr').json()

    for ticker in main_data:
        if 'closeTime' in ticker and ticker['closeTime'] is not None:
            try:
                ticker['openPositionDay'] = datetime.fromtimestamp(ticker['closeTime'] / 1000).strftime(
                    '%d-%m-%Y')
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
        database.connect()

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
        now = datetime.now()

        try:
            rounded_time = now.replace(second=0, microsecond=0)

            last_5 = sorted_data_funding[0:5]
            first_5 = sorted_data_funding[-5:]
            print("Check point 1!")

            for record in first_5:
                stock_id = database.execute_with_return(
                    """
                        SELECT stock_id
                        FROM data_history.funding
                        WHERE symbol = %s 
                    """, (record['symbol'],),
                )

                if not stock_id:
                    continue

                stock_id = stock_id[0][0]

                quote_volume_5m = database.execute_with_return(
                    """
                    SELECT funding_rate
                    FROM data_history.funding_data
                    WHERE stock_id = %s
                    ORDER BY funding_time DESC
                    LIMIT 1 OFFSET 4;
                    """, (stock_id,)
                )

                if quote_volume_5m:
                    record['5_min_value'] = float(quote_volume_5m[0][0])

            print("Check point 2!")

            for record in last_5:
                stock_id = database.execute_with_return(
                    """
                        SELECT stock_id
                        FROM data_history.funding
                        WHERE symbol = %s 
                    """, (record['symbol'],),
                )

                if not stock_id:
                    continue

                stock_id = stock_id[0][0]

                quote_volume_5m = database.execute_with_return(
                    """
                    SELECT funding_rate
                    FROM data_history.funding_data
                    WHERE stock_id = %s
                    ORDER BY funding_time
                    LIMIT 1 OFFSET 4;
                    """, (stock_id,)
                )

                if quote_volume_5m:
                    record['5_min_value'] = float(quote_volume_5m[0][0])

            print("Check point 3!")

            redis_data = {
                'last_update_time': rounded_time.isoformat(),
                'first_5': first_5,
                'last_5': last_5,
            }

            print("Check point 4!")

            json_data = json.dumps(redis_data)
            print("Check point 5!")

            redis_database.set('funding:top:5:tickets', json_data)
        except Exception as e:
            logging.error(f"Error arose while trying to insert top tickets into Reddis, error message:{e}")

        for record in sorted_data_funding:
            try:
                stock_id = database.execute_with_return(
                    """
                        WITH ins AS (
                            INSERT INTO data_history.funding (symbol, company_name)
                            VALUES (%s, %s)
                            ON CONFLICT (symbol) DO NOTHING
                            RETURNING stock_id
                        )
                        SELECT stock_id FROM ins
                        UNION ALL
                        SELECT stock_id FROM data_history.funding
                        WHERE symbol = %s AND NOT EXISTS (SELECT 1 FROM ins)
                    """, (record["symbol"], "crypto", record["symbol"])
                )
            except Exception as e:
                logging.error(f"Error arose while trying to insert funding names into DB, error message:{e}")
                return "Error with DB"

            stock_id = stock_id[0][0]
            funding_rate = record["lastFundingRate"]
            market_price = record["markPrice"]

            seconds = record["time"] / 1000.0
            time_value = datetime.fromtimestamp(seconds, tz=timezone.utc)

            try:
                records_count = database.execute_with_return(
                    """
                        SELECT COUNT(*) FROM data_history.funding_data WHERE stock_id = %s;
                    """, (stock_id,)
                )

                if records_count[0][0] >= 43200:
                    database.execute("""
                                DELETE FROM data_history.funding_data
                                WHERE stock_id = (
                                    SELECT stock_id FROM data_history.funding_data
                                    WHERE stock_id = %s
                                    ORDER BY funding_time ASC
                                    LIMIT 1
                                );
                            """, (stock_id,))

                    print(f"Deleted the oldest record with column value '{stock_id}'")

                database.execute(
                    """
                        INSERT INTO data_history.funding_data (stock_id, funding_rate, mark_price, funding_time)
                        VALUES (%s, %s, %s, %s)
                    """, (stock_id, funding_rate, market_price, time_value)
                )

            except Exception as e:
                logging.error(f"Error arose while trying to insert funding data into DB, error message:{e}")
                return "Error with DB"

        database.disconnect()


schedule.every(60).seconds.do(get_funding_data)
schedule.every(60).seconds.do(get_volume_data)

while True:
    schedule.run_pending()
    time.sleep(1)

