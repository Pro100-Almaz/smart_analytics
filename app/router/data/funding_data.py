from datetime import datetime
from typing import Dict

import os
import csv
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import requests

from fastapi import APIRouter, HTTPException, status, Depends, Query

from app.database import database
from app.auth_bearer import JWTBearer


load_dotenv()
router = APIRouter()


@router.get("/funding_data", tags=["data"])
async def get_funding_data(interval: int = Query(7), token_data: Dict = Depends(JWTBearer())):
    funding_response = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex")
    if funding_response.status_code == 200:
        user_id = token_data.get("user_id")
        csv_file_path = f"dataframes/funding_data_{user_id}.csv"

        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["symbol", "fundingTime", "fundingRate", "markPrice"])

        return_value = {
            'time_interval': [0],
            'positive': [0],
            'negative': [0],
            'neutral': [0]
        }
        positive_funding_rate_quantity = 0
        negative_funding_rate_quantity = 0
        neutral_funding_rate_quantity = 0

        if interval == 1:
            time_gap = 60
        elif interval == 7:
            time_gap = 480
        else:
            time_gap = 1440

        funding_data = funding_response.json()

        for record in funding_data:
            if float(record['lastFundingRate']) > 0.01:
                positive_funding_rate_quantity += 1
                return_value['positive'][0] += 1
            elif float(record['lastFundingRate']) != 0.005 and float(record['lastFundingRate']) < 0.01:
                negative_funding_rate_quantity += 1
                return_value['negative'][0] += 1
            elif float(record['lastFundingRate']) == 0.01 or float(record['lastFundingRate']) == 0.05:
                neutral_funding_rate_quantity += 1
                return_value['neutral'][0] += 1

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
                    funding_time;
                """, stock_id, time_gap
            )

            for data in stock_data:
                if data.get("rn") not in return_value['time_interval']:
                    return_value['time_interval'].append(data.get("rn"))
                    return_value['positive'].append(0)
                    return_value['negative'].append(0)
                    return_value['neutral'].append(0)

                if data.get('funding_rate') > 0.01:
                    return_value['positive'][-1] += 1
                if 0.005 != data.get('funding_rate') < 0.01:
                    return_value['negative'][-1] += 1
                if data.get('funding_rate') == 0.01 or data.get('funding_rate') == 0.05:
                    return_value['neutral'][-1] += 1

            with open(csv_file_path, mode='a', newline='') as file:
                writer = csv.writer(file)

                writer.writerow([record['symbol'], record['time'], float(record['lastFundingRate']) * 100, record['markPrice']])

        await database.execute(
            """
            INSERT INTO data_history.funding_data_history (user_id, created, positive_count, negative_count, neutral_count)
            VALUES ($1, $2, $3, $4, $5);
            """, user_id, datetime.now(), positive_funding_rate_quantity, negative_funding_rate_quantity, neutral_funding_rate_quantity
        )

        return {"status": status.HTTP_200_OK,
                "positive_quantity": positive_funding_rate_quantity,
                "negative_quantity": negative_funding_rate_quantity,
                "neutral_quantity": neutral_funding_rate_quantity,
                "graph_data": return_value
                }

    return {"status": "service_error"}


@router.get("/funding_data_history", tags=["data"])
async def get_funding_history(token_data: Dict = Depends(JWTBearer())):
    data = await database.fetch(
        """
        SELECT created, positive_count, negative_count, neutral_count
        FROM data_history.funding_data_history
        WHERE user_id = $1
        """, token_data.get("user_id")
    )

    return {"status": status.HTTP_200_OK, "data": data}

