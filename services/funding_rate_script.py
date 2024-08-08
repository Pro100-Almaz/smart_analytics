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


def get_funding_data():
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    response = requests.get(url)
    if response.status_code == 200:
        database.connect()

        funding_data = response.json()

        sorted_data = sorted(funding_data, key=lambda x: float(x['fundingRate']))
        now = datetime.datetime.now()

        try:
            rounded_time = now.replace(second=0, microsecond=0)

            first_5 = sorted_data[0:5]
            last_5 = sorted_data[-5:]

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

while True:
    schedule.run_pending()
    time.sleep(1)

