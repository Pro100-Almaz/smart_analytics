import os
import logging
from logging.handlers import RotatingFileHandler
import json
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from database import database, redis_database


log_directory = "logs"
log_filename = "notification.log"
log_file_path = os.path.join(log_directory, log_filename)

if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

handler = RotatingFileHandler(log_file_path, maxBytes=2000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def save_websocket_data(websocket_data: dict):
    logger.info("Saving websocket data, active name: ", websocket_data.get('s', "No data"))
    database.connect()
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
            """, (websocket_data.get('s'), "crypto", websocket_data.get('s'))
        )
    except Exception as e:
        logger.error("Error in query for selecting stock id of active, message: ", e)
        return "Error with DB"

    stock_id = stock_id[0][0]

    open_time = datetime.fromtimestamp(websocket_data.get('t') / 1000.0)
    close_time = datetime.fromtimestamp(websocket_data.get('T')  / 1000.0)

    try:
        records_count = database.execute_with_return(
            """
                SELECT COUNT(*) FROM data_history.kline_1 WHERE stock_id = %s;
            """, (stock_id,)
        )

        if records_count[0][0] >= 87840:
            database.execute("""
                                DELETE FROM data_history.kline_1
                                WHERE stock_id = (
                                    SELECT stock_id FROM data_history.volume_data
                                    WHERE stock_id = %s
                                    ORDER BY open_time ASC
                                    LIMIT 1440
                                );
                """, (stock_id,))

            print(f"Deleted the oldest record with column value '{stock_id}'")

        database.execute(
            """
                INSERT INTO data_history.kline_1 (stock_id, open_time, close_time, interval, open_price, close_price, 
                highest_price, lowest_price, volume_token, volume_dollar, volume_market)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (stock_id, open_time, close_time, websocket_data.get('i'), Decimal(websocket_data.get('o')),
                  Decimal(websocket_data.get('c')), Decimal(websocket_data.get('h')), Decimal(websocket_data.get('l')),
                  Decimal(websocket_data.get('v')), Decimal(websocket_data.get('q')), Decimal(websocket_data.get('V')))
        )
    except Exception as e:
        logger.error("Error in record saving query, message is: ", e)
        return "Error with DB"

    database.disconnect()

