import json
from datetime import datetime, timezone, timedelta
import logging
import requests
import ssl
import os
import asyncio
import multiprocessing
from logging.handlers import RotatingFileHandler
import aiohttp
from dotenv import load_dotenv
from tasks import update_stock_data, push_stock_data
from utils import save_websocket_data

log_directory = "logs"
log_filename = "candlestick_receiver.log"
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

checker_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "TONUSDT", "BNBUSDT"]


def get_symbols():
    """Fetch the asset symbols from Binance."""
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
    """Convert Unix timestamp to readable date."""
    timestamp_in_seconds = unix / 1000
    utc_time = datetime.fromtimestamp(timestamp_in_seconds, tz=timezone.utc)
    adjusted_time = utc_time + timedelta(hours=5)
    return adjusted_time.strftime('%d-%m-%Y | %H:%M')


def get_chunk_of_data(l, n):
    """Divide the list into chunks."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


async def get_assets_ohlc(proxy, chunk_of_assets):
    """Handle WebSocket connection and processing of assets."""
    logger.info("The amount of assets in current thread is: %s", len(chunk_of_assets))
    for asset in chunk_of_assets:
        logger.info(f"Asset: {asset}")

    asset_streams = [f"{str(asset).lower()}@kline_1m" for asset in chunk_of_assets]
    uri = f"wss://fstream.binance.com/stream?streams={'/'.join(asset_streams)}"
    phase_minute = None

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(uri, proxy=proxy) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            active_data = json.loads(msg.data)
                            current_time = unix_to_date(active_data.get('data', {}).get('k', {}).get('T'))
                            active_name = active_data.get('data', {}).get('s')
                            last_value = float(active_data.get('data', {}).get('k', {}).get('c'))

                            if active_name in checker_list:
                                logger.info(f"Time given in websocket: {current_time}, last value: {last_value}, active name: {active_name}")

                            if phase_minute != current_time:
                                phase_minute = current_time
                                push_stock_data.delay(active_name, last_value)
                                save_websocket_data(active_data.get('data', {}).get('k', {}))
                            else:
                                update_stock_data.delay(active_name, last_value)

                        elif msg.type == aiohttp.WSMsgType.PING:
                            await ws.pong(msg.data)
                        elif msg.type == aiohttp.WSMsgType.PONG:
                            logger.info("Pong received")
                        elif msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                            logger.error("WebSocket closed or error")
                            break
        except Exception as e:
            logger.error(f"An error occurred while processing data at proxy {proxy}: {e}")
            await asyncio.sleep(5)

        logger.error(f"Reconnecting to Binance using proxy {proxy}...")


def run_websocket_process(proxy, chunk_of_assets):
    """Run WebSocket connections for a chunk of assets in a separate process."""
    asyncio.run(get_assets_ohlc(proxy, chunk_of_assets))


def main():
    # Fetch all symbols
    the_beginning_time = time.time()
    symbols = get_symbols()

    # Split symbols into 5 chunks
    chunked_assets = list(get_chunk_of_data(symbols, len(symbols) // 5))

    # Start 5 separate processes for WebSocket tracking
    processes = []
    for i in range(5):
        p = multiprocessing.Process(target=run_websocket_process, args=(proxy_list[i], chunked_assets[i]))
        processes.append(p)
        p.start()

    # Wait for all processes to complete
    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
