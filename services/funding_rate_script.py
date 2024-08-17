import json
from decimal import Decimal, ROUND_DOWN

import requests
import datetime
import os
import time

import schedule
import logging
from logging.handlers import RotatingFileHandler

from database import database, redis_database
from tasks import update_stock_data, get_stock_data


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
    volume_response = requests.get('https://fapi.binance.com/fapi/v1/ticker/24hr')
    if volume_response.status_code == 200:
        database.connect()
        volume_data = volume_response.json() if volume_response.status_code == 200 else None
        updated_volume_data = [d for d in volume_data if d['symbol'] not in except_list]

        sorted_data_volume = sorted(updated_volume_data, key=lambda x: float(x['quoteVolume']))
        now = datetime.datetime.now()

        try:
            rounded_time = now.replace(second=0, microsecond=0)

            first_5 = sorted_data_volume[-5:]

            redis_data = {
                'last_update_time': rounded_time.isoformat(),
                'first_5': first_5,
            }

            json_data = json.dumps(redis_data)
            redis_database.set('funding:top:5:tickets:volume', json_data)
        except Exception as e:
            logging.error(f"Error arose while trying to insert top tickets into Reddis, error message:{e}")

        for record in volume_data:
            update_stock_data.delay(record["symbol"], record["lastPrice"])

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

            open_time = datetime.datetime.fromtimestamp(record["openTime"] / 1000.0)
            close_time = datetime.datetime.fromtimestamp(record["closeTime"] / 1000.0)

            try:
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
        return True

    return False


def get_funding_data():
    funding_response = requests.get("https://fapi.binance.com/fapi/v1/fundingRate")
    if funding_response.status_code == 200:
        database.connect()

        funding_data = funding_response.json() if funding_response.status_code == 200 else None

        sorted_data_funding = sorted(funding_data, key=lambda x: float(x['fundingRate']))
        now = datetime.datetime.now()

        try:
            rounded_time = now.replace(second=0, microsecond=0)

            last_5 = sorted_data_funding[0:5]
            first_5 = sorted_data_funding[-5:]

            redis_data = {
                'last_update_time': rounded_time.isoformat(),
                'first_5': first_5,
                'last_5': last_5,
            }

            json_data = json.dumps(redis_data)
            redis_database.set('funding:top:5:tickets', json_data)
        except Exception as e:
            logging.error(f"Error arose while trying to insert top tickets into Reddis, error message:{e}")

        for record in funding_data:
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
            funding_rate = Decimal(record["fundingRate"]).quantize(Decimal('.00000000001'), rounding=ROUND_DOWN)
            market_price = Decimal(record["markPrice"]).quantize(Decimal('.00000001'), rounding=ROUND_DOWN)

            seconds = record["fundingTime"] / 1000.0
            timestamp = datetime.datetime.fromtimestamp(seconds)

            try:
                database.execute(
                    """
                        INSERT INTO data_history.funding_data (stock_id, funding_rate, mark_price, funding_time)
                        VALUES (%s, %s, %s, %s)
                    """, (stock_id, funding_rate, market_price, timestamp)
                )
            except Exception as e:
                logging.error(f"Error arose while trying to insert funding data into DB, error message:{e}")
                return "Error with DB"

        database.disconnect()
        return True

    return False

schedule.every(60).seconds.do(get_funding_data)
schedule.every(30).seconds.do(get_volume_data)

while True:
    schedule.run_pending()
    time.sleep(1)

