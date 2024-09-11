import os
import csv
import requests
from datetime import datetime
from typing import Dict

from dotenv import load_dotenv

from fastapi import APIRouter, HTTPException, status, Depends, Query, BackgroundTasks

from app.database import database
from app.auth_bearer import JWTBearer


load_dotenv()
router = APIRouter()


def calculate_percentage_change(value_1, value_2):
    if value_2:
        return round((value_1 - value_2) / abs(value_2) * 100, 2)
    return round(value_1 * 100, 2)


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


@router.get("/gradation_growth", tags=["data"])
async def get_gradation(background_tasks: BackgroundTasks, interval: int = Query(30), growth_type: str = Query("Volume", max_length=50), token_data: Dict = Depends(JWTBearer())):
    volume_response = requests.get('https://fapi.binance.com/fapi/v1/ticker/24hr')
    if volume_response.status_code == 200:
        volume_data = volume_response.json()
        user_id = token_data.get("user_id")
        current_date = datetime.now().date()
        current_time = datetime.now().time().replace(microsecond=0)

        directory_path = f"dataframes/{user_id}/{current_date}/{current_time}"
        os.makedirs(directory_path, exist_ok=True)
        if growth_type == "Volume":
            csv_file_path = directory_path + f"/volume_growth_{interval}.csv"
            file_name = f"volume_growth_{interval}.csv"
        else:
            csv_file_path = directory_path + f"/price_growth_{interval}.csv"
            file_name = f"price_growth_{interval}.csv"

        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["symbol", "local_volume_change_percent"])

        file_id = await database.fetch(
            """
            INSERT INTO data_history.growth_data_history (user_id, date, time, file_name, type)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING file_id;
            """, user_id, current_date, current_time, file_name, "volume" if growth_type == "Volume" else "price"
        )

        background_tasks.add_task(file_generation, volume_data, interval, growth_type, csv_file_path)

        return {"status": status.HTTP_200_OK, "file_name": f"volume_growth_{interval}.csv", "file_id": file_id[0][0]}

    return {"status": "service_error"}


@router.get("/gradation_growth_history", tags=["data"])
async def get_gradation_history(growth_type: str = Query("Volume", max_length=50), token_data: Dict = Depends(JWTBearer())):
    data = await database.fetch(
        """
        SELECT date, time, file_name, file_id
        FROM data_history.growth_data_history
        WHERE user_id = $1 AND type = $2
        LIMIT 10;
        """, token_data.get("user_id"), "volume" if growth_type == "Volume" else "price"
    )

    return {"status": status.HTTP_200_OK, "data": data}

