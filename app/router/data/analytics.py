from typing import Dict

from dotenv import load_dotenv

from fastapi import APIRouter, HTTPException, status, Depends, Query

from app.database import database
from app.auth_bearer import JWTBearer
from schemas import VolumeData


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


@router.get("/volume_24hr", dependencies=[Depends(JWTBearer())])
async def volume_24hr(params: VolumeData):

    return True
