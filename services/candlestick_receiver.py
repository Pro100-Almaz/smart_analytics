import json
from datetime import datetime, timezone, timedelta
import logging
from logging.handlers import RotatingFileHandler
import requests
import asyncio
import aiohttp
import ssl
import os
import time

from dotenv import load_dotenv
from tasks import update_stock_data, push_stock_data
from notification import last_impulse_notification
from utils import save_websocket_data


log_directory = "logs"
log_filename = "candlestick_receiver.log"
log_file_path = os.path.join(log_directory, log_filename)

if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = RotatingFileHandler(log_file_path, maxBytes=2000, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

load_dotenv()

proxies_string = os.getenv('PROXIES')

proxy_list = proxies_string.split(',')


def get_symbols():
    main_data = requests.get('https://fapi.binance.com/fapi/v1/ticker/24hr').json()
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

    not_perpetual_symbols = [record['symbol'] for record in exchange_info_data if record['contractType'] != 'PERPETUAL']

    full_symbol_list_to_delete = set(not_usdt_symbols + delete_symbols + not_perpetual_symbols)

    for i in range(len(main_data)):
        if main_data[i]['symbol'] not in full_symbol_list_to_delete:
            result_list.append(main_data[i]['symbol'])

    result_list = sorted(result_list)
    return result_list


def unix_to_date(unix):
    timestamp_in_seconds = unix / 1000

    utc_time = datetime.fromtimestamp(timestamp_in_seconds, tz=timezone.utc)

    adjusted_time = utc_time + timedelta(hours=5)

    # Format the adjusted time
    date = adjusted_time.strftime('%d-%m-%Y | %H:%M')
    return date


def get_chunk_of_data(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


checker_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "TONUSDT", "BNBUSDT"]


async def get_assets_ohlc(proxy, chunk_of_assets, ssl_context=None):
    logger.info("The amount of assets in current thread is: %s", len(chunk_of_assets))
    asset_streams = [f"{str(asset).lower()}@kline_1m" for asset in chunk_of_assets]
    uri = f"wss://fstream.binance.com/stream?streams={'/'.join(asset_streams)}"
    phase_minute = None

    while True:
        try:
            # timeout = aiohttp.ClientTimeout(sock_read=10)
            async with aiohttp.ClientSession() as session: # aiohttp.ClientSession(timeout=timeout)
                async with session.ws_connect(uri, proxy=proxy) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            active_data = json.loads(msg.data)
                            current_time = unix_to_date(active_data.get('data', {}).get('k', {}).get('T'))
                            active_name = active_data.get('data', {}).get('s')
                            last_value = float(active_data.get('data', {}).get('k', {}).get('c'))

                            if active_name in checker_list:
                                logger.info(f"Time given in websocket: {current_time}, last value: {last_value}, active name: {active_name}")

                            # if phase_minute != current_time:
                            #     phase_minute = current_time
                            #     await asyncio.gather(
                            #         push_stock_data.delay(active_name, last_value),
                            #         save_websocket_data(active_data.get('data', {}).get('k', {}))
                            #     )
                            # else:
                            #     update_stock_data.delay(active_name, last_value)

                                # if res == "create_stock_key":
                                #     await asyncio.gather(
                                #         push_stock_data.delay(active_name, last_value),
                                #         save_websocket_data(active_data.get('data', {}).get('k', {}))
                                #     )

                            if phase_minute != current_time:
                                phase_minute = current_time
                                push_stock_data.delay(active_name, last_value)
                                save_websocket_data(active_data.get('data', {}).get('k', {}))

                            else:
                                update_stock_data.delay(active_name, last_value)

                                # if res == "create_stock_key":
                                #     push_stock_data.delay(active_name, last_value)
                                #     save_websocket_data(active_data.get('data', {}).get('k', {}))

                            try:
                                last_impulse_notification()
                            except Exception as e:
                                print("Error while sending notification: ", e)

                            if active_name in checker_list:
                                logger.info("Process ended!")

                        elif msg.type == aiohttp.WSMsgType.PING:
                            await ws.pong(msg.data)
                            logger.info("Ping received and pong sent")
                        elif msg.type == aiohttp.WSMsgType.PONG:
                            logger.info("Pong received")
                        elif msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                            logger.error("WebSocket closed/error")
                            break
        except Exception as e:
            logger.error(f"An error occurred while processing data, at proxy {proxy}: {e}")
            print("Error: ", e)
            await asyncio.sleep(5)

        logger.error(f"Reconnecting to Binance using proxy {proxy}...")


# async def handle_exit(signum, frame):
#     await send_message('Script: get_asset_data\n'
#                  'Text: Script is being stopped manually.')
#     raise SystemExit
#
#
# signal.signal(signal.SIGINT, handle_exit)
# signal.signal(signal.SIGTERM, handle_exit)


async def update_symbols(queue):
    while True:
        logger.info("--> Updating symbols!")
        symbols = get_symbols()
        await queue.put(symbols)
        await asyncio.sleep(86400)


async def dispatcher(queue, proxies):
    while True:
        assets = await queue.get()
        chunk_of_assets = list(get_chunk_of_data(assets, len(assets) // len(proxies)))

        tasks = [get_assets_ohlc(proxies[i], chunk_of_assets[i]) for i in range(len(proxies))]
        await asyncio.gather(*tasks)


async def main():
    # directories = ["dataframes", "dataframes/raw_data"]
    # for directory in directories:
    #     os.makedirs(directory, exist_ok=True)

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    queue = asyncio.Queue()

    id_retrieving_symbols_function = asyncio.create_task(update_symbols(queue))
    dispatcher_task = asyncio.create_task(dispatcher(queue, proxy_list))

    await asyncio.gather(id_retrieving_symbols_function, dispatcher_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass