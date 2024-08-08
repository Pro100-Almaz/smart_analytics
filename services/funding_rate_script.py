import json
import requests
import datetime
import logging

from database import database, redis_database

logging.basicConfig(filename='logs\\funding_rate.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

print("Hello")

def get_funding_data():
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    response = requests.get(url)
    if response.status_code == 200:
        database.connect()

        funding_data = response.json()

        sorted_data = sorted(funding_data, key=lambda x: float(x['fundingRate']))
        now = datetime.datetime.now()

        rounded_time = now.replace(second=0, microsecond=0)

        first_5 = sorted_data[0:5]
        last_5 = sorted_data[-5:]

        redis_data = {
            'last_update_time': rounded_time,
            'first_5': first_5,
            'last_5': last_5,
        }

        json_data = json.dumps(redis_data)
        redis_database.set('funding:top:5:tickets', json_data)

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
                logging.error(f"This is an error message {e}")

            stock_id = stock_id[0][0]
            funding_rate = record["fundingRate"]
            market_price = record["markPrice"]

            seconds = record["fundingTime"] / 1000.0
            timestamp = datetime.datetime.fromtimestamp(seconds)

            database.execute(
                """
                    INSERT INTO data_history.funding_data (stock_id, funding_rate, mark_price, funding_time)
                    VALUES (%s, %s, %s, %s);
                """, (stock_id, funding_rate, market_price, timestamp)
            )

        return funding_data

    return False

data = get_funding_data()
# top_5_negative_funding = data.head(5)['symbol'].tolist()
# top_5_positive_funding = data.tail(5)['symbol'].tolist()
