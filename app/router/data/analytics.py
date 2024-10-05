import csv
import os
from datetime import datetime
from typing import Dict

from dotenv import load_dotenv

from fastapi import APIRouter, HTTPException, status, Depends, Query

from app.database import database
from app.auth_bearer import JWTBearer
from .schemas import VolumeData
from app.webhook import bot


load_dotenv()
router = APIRouter()


def calculate_percentage_change(value_1, value_2):
    if value_2:
        return round((value_1 - value_2) / abs(value_2) * 100, 2)
    return round(value_1 * 100, 2)


def format_number(number):
    integer_part, fractional_part = f"{number:.2f}".split('.')
    formatted_integer = f'{int(integer_part):,}'
    return f"{formatted_integer}.{fractional_part}$"


async def file_generation(volume_data, interval, growth_type, csv_file_path):
    for record in volume_data:
        stock_id = await database.fetchrow(
            """
            SELECT stock_id
            FROM data_history.funding
            WHERE symbol = $1;
            """, record["symbol"]
        )

        stock_id = stock_id.get("stock_id")

        stock_data = await database.fetch(
            """
            WITH FilteredData AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (ORDER BY open_time) AS rn
                FROM
                    data_history.volume_data
                WHERE
                    stock_id = $1
            )
            SELECT
                *
            FROM
                FilteredData
            WHERE
                rn % $2 = 0  
            ORDER BY
                open_time
            LIMIT 1;
            """, stock_id, interval
        )

        stock_data = stock_data[0]

        if growth_type == "Volume":
            local_percent = calculate_percentage_change(float(record["quoteVolume"]), float(stock_data["quote_volume"]))
        else:
            local_percent = calculate_percentage_change(float(record["lastPrice"]), float(stock_data["last_price"]))

        with open(csv_file_path, mode='a', newline='') as file:
            writer = csv.writer(file)

            writer.writerow([record["symbol"], local_percent])


@router.get("/ticker_information", dependencies=[Depends(JWTBearer())])
async def ticker_information(ticker: str = Query(max_length=50)):
    if not ticker:
        return {"status": "fail", "message": "No ticker provided"}

    ticker_exists = await database.fetchrow(
        """
        SELECT *
        FROM data_history.funding
        WHERE symbol = $1;
        """, ticker
    )

    if not ticker_exists:
        return {"status": "fail", "message": "No such ticker"}

    btc_data = await database.fetch(
        """
            WITH btcusdt AS (
                SELECT stock_id
                FROM data_history.funding
                WHERE symbol = $1
            )
            SELECT *
            FROM data_history.kline_1
            WHERE stock_id = btcusdt
            ORDER BY open_time DESC
            LIMIT 1440;
        """, "BTCUSDT"
    )

    ticker_data = await database.fetch(
        """
            WITH btcusdt AS (
                SELECT stock_id
                FROM data_history.funding
                WHERE symbol = $1
            )
            SELECT *
            FROM data_history.kline_1
            WHERE stock_id = btcusdt
            ORDER BY open_time DESC
            LIMIT 1440;
        """, ticker
    )

    ticker_30_days_data = await database.fetch(
        """
            WITH btcusdt AS (
                SELECT stock_id
                FROM data_history.funding
                WHERE symbol = $1
            )
            SELECT *
            FROM data_history.kline_1
            WHERE stock_id = btcusdt
            ORDER BY open_time DESC
            LIMIT 43200;
        """, ticker
    )

    cumulative_sum = 0

    # Вычисление суточного объёма
    for record in ticker_data:
        cumulative_sum += record.get('volume_dollar')
        record['daily_volume'] = round(cumulative_sum, 2)
        record['candle_amplitude'] = round((record.get('close_price') * 100 / record.get('close_price')) - 100, 2)

    current_price = format_number(ticker_data[-1]['close_price'])  # текущая цена
    current_daily_volume = ticker_data[-1]['daily_volume']  # текущий объём торгов

    # Вычисление амплитуды свечи для BTC
    for record in btc_data:
        record['candle_amplitude'] = round((record.get('close_price') * 100 / record.get('close_price')) - 100, 2)

    # Вычисление среднего значения амплитуды
    mean_ticker = sum(item['candle_amplitude'] for item in ticker_data) / len(ticker_data)
    mean_btc = sum(item['candle_amplitude'] for item in btc_data) / len(btc_data)

    # Вычисление отклонений
    ticker_deviation = [item['candle_amplitude'] - mean_ticker for item in ticker_data]
    btc_deviation = [item['candle_amplitude'] - mean_btc for item in btc_data]

    # Вычисление числителя и знаменателя для корреляции Пирсона
    numerator = sum(ticker_deviation[i] * btc_deviation[i] for i in range(len(ticker_deviation)))
    denominator = (sum(val ** 2 for val in ticker_deviation) * sum(val ** 2 for val in btc_deviation)) ** 0.5

    pearson_correlation = round((numerator / denominator) * 100, 1)  # корреляция тикера и BTCUSDT

    # Вычисление медианного суточного объёма за 30 дней
    sorted_volumes = sorted(item['q'] for item in ticker_30_days_data)
    median_daily_volume = round(sorted_volumes[len(sorted_volumes) // 2], 2) if len(sorted_volumes) % 2 != 0 else \
        round((sorted_volumes[len(sorted_volumes) // 2 - 1] + sorted_volumes[len(sorted_volumes) // 2]) / 2, 2)

    volume_difference = calculate_percentage_change(value_1=current_daily_volume, value_2=median_daily_volume)
    current_daily_volume = format_number(current_daily_volume)  # текущий объём торгов с разделителями

    return {
        "status": status.HTTP_200_OK,
        "current_price": current_price,
        "current_daily_volume": current_daily_volume,
        "pearson_correlation": pearson_correlation,
        "volume_difference": volume_difference,
        "median_daily_volume": median_daily_volume,
        "ticker_data": ticker_data
    }


@router.get("/volume_24hr", tags=["analytics"])
async def volume_24hr(params: VolumeData, action: str = Query(max_length=20, default="generate"), token_data: Dict = Depends(JWTBearer())):
    ticker = await database.fetchrow(
        """
        SELECT *
        FROM data_history.funding
        WHERE symbol = $1;
        """, params.active_name
    )

    if not ticker:
        return {"status": status.HTTP_404_NOT_FOUND, "message": "No such ticker!"}

    time_gap = 60 if params.time_value <= 3 else 1440
    limit_number = 24 * params.time_value if params.time_value <= 3 else params.time_value

    try:
        stock_data = await database.fetch(
            """
            WITH FilteredData AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (ORDER BY funding_time) AS rn
                FROM
                    data_history.funding_data
                WHERE
                    stock_id = $1
            )
            SELECT
                *
            FROM
                FilteredData
            WHERE
                rn % $2 = 0  
            ORDER BY
                funding_time
            LIMIT
                $3;
            """, ticker.get('stock_id'), time_gap, limit_number
        )
    except Exception as e:
        return {"status": status.HTTP_409_CONFLICT, "message": "Error occurred while processing the data from database!"}

    if action == "generate":
        return_value = {
            'time_interval': [],
            'volume_data': []
        }

        for data in stock_data:
            return_value['time_interval'].append(data['close_time'])
            return_value['volume_data'].append(data['volume'])

        difference_percent = (stock_data[-1]['volume'] - stock_data[0]['volume']) / stock_data[0]['volume'] * 100

        return {"status": status.HTTP_200_OK, "data": return_value,
                "last_update": datetime.now().date(), "difference_percent":difference_percent}

    if action == "sent":
        user_id = token_data.get("user_id")
        current_date = datetime.now().date()
        current_time = datetime.now().time().replace(microsecond=0)

        directory_path = f"dataframes/{user_id}/{current_date}/{current_time}"
        os.makedirs(directory_path, exist_ok=True)
        csv_file_path = directory_path + f"/{params.time_value}d_volume.csv"

        last_value = None
        row_index = 0

        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["index", "date", "daily_volume", "volume_change_percent"])

            for data in stock_data[-1:0:-1]:
                date = data['close_time'].strftime("%d-%m-%Y | %H:%M")
                change_percent = None

                if last_value:
                    change_percent = (last_value - data['volume']) / data['volume'] * 100
                    last_value = data['volume']

                writer.writerow([row_index, date, data['daily_volume'], change_percent])

        telegram_id = token_data["telegram_id"]

        with open(csv_file_path, 'rb') as file:
            await bot.send_document(chat_id=telegram_id, document=file, filename="funding_data.csv")

        return {"Status": "ok"}

    return {}
