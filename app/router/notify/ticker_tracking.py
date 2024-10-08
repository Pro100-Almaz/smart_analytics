import requests
import statistics

from datetime import datetime
from typing import Dict

from dotenv import load_dotenv

from fastapi import APIRouter, HTTPException, status, Depends, Query

from app.database import database
from app.auth_bearer import JWTBearer
from .schemas import TickerTracking

import json
from html.parser import HTMLParser


load_dotenv()
router = APIRouter()


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


@router.get("/get_ticker_tracking", tags=["notify"])
async def get_ticker_tracking(token_data: Dict = Depends(JWTBearer())):
    records = await database.fetch(
        """
        SELECT *
        FROM users.user_notification
        WHERE user_id = $1 AND notification_type = 'ticker_tracking' AND active = true
        """, token_data.get("user_id")
    )

    conditions = []
    for record in records:
        time, ticker = record.get("condition").split(":")
        conditions.append({
            "id": record.get("id"),
            "time": time.split("_")[0],
            "ticker": ticker
        })

    return {"status": status.HTTP_200_OK, "records": records, "conditions": conditions}


@router.get("/get_ticker_tracking_history", tags=["notify"])
async def get_ticker_tracking_history(tt_id: int = Query()):
    ticker_tracking_history = await database.fetch(
        f"""
        SELECT *
        FROM users.notification
        WHERE type = $1
        ORDER BY date DESC 
        LIMIT 10;
        """, tt_id
    )

    return_data = []

    for item in ticker_tracking_history:
        temp_value = {}
        params_data = json.loads(item["params"])
        params_data.pop("type")
        params_data.pop("telegram_id")

        temp_value["type"] = item["type"]
        temp_value["date"] = item["date"]
        temp_value["status"] = item["status"]
        temp_value["active_name"] = item["active_name"]
        temp_value["params"] = params_data

        return_data.append(temp_value)

    return {"status": status.HTTP_200_OK, "ticker_tracking_history": return_data}


@router.patch("/update_ticker_tracking", tags=["notify"], dependencies=[Depends(JWTBearer())])
async def get_impulse_history(tt_params: TickerTracking, tt_id: int = Query()):
    condition = f"{tt_params.time_period}_min:{tt_params.ticker_name}"

    try:
        await database.execute(
            f"""
                UPDATE users.user_notification
                SET condition = $2
                WHERE id = $1
                """, tt_id, condition
        )
    except Exception as e:
        return {"status": status.HTTP_400_BAD_REQUEST, "message": "Error updating impulse!"}

    return {"status": status.HTTP_200_OK}


@router.delete("/delete_ticker_tracking", tags=["notify"])
async def delete_ticker_tracking(tt_id: int = Query(None), token_data: Dict = Depends(JWTBearer())):
    if tt_id is None:
        return {"status": "error", "message": "No ticker tracking id"}

    try:
        await database.execute(
            """
            UPDATE users.user_notification
            SET active = false
            WHERE user_id = $1 AND id = $2
            """, token_data.get("user_id"), tt_id
        )
    except:
        return {"status": "error", "message": "Ticker tracking not found"}

    return {"status": status.HTTP_200_OK, "message": "Ticker tracking deleted"}


@router.post("/set_ticker_tracking", tags=["notify"])
async def set_ticker_tracking(tt_params: TickerTracking, token_data: Dict = Depends(JWTBearer())):
    ticker_name = await database.fetchrow(
        """
        SELECT *
        FROM data_history.funding
        WHERE symbol = $1;
        """, tt_params.ticker_name
    )

    if not ticker_name:
        return {"status": status.HTTP_400_BAD_REQUEST, "message": "Ticker tracking not found!"}

    status_to_add = await database.fetch(
        """
            WITH notification_count AS (
                SELECT COUNT(*) AS count
                FROM users.user_notification
                WHERE user_id = $1 AND notification_type = 'ticker_tracking' AND active = true
            )
            SELECT 
                CASE 
                    WHEN p.status = true THEN nc.count < 3
                    ELSE nc.count < 1
                END AS allowed_to_add
            FROM users.premium p
            CROSS JOIN notification_count nc
            WHERE p.user_id = $1;
        """, token_data.get("user_id")
    )

    condition = f"{tt_params.time_period}_min:{tt_params.ticker_name}"

    if not status_to_add[0].get("allowed_to_add"):
        return {"status": status.HTTP_403_FORBIDDEN, "message": "Ticker tracking not allowed!"}

    try:
        await database.execute(
            """
            INSERT INTO users.user_notification (user_id, notification_type, notify_time, condition, created) 
            VALUES ($1, 'ticker_tracking', NULL, $2, $3)
            """, token_data.get("user_id"), condition, datetime.now()
        )
    except Exception as e:
        print(e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Error arose while adding new notification. Condition 1"
        )

    return {"Success": status.HTTP_200_OK}