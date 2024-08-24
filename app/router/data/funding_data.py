from datetime import datetime
from typing import Dict

import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2

from fastapi import APIRouter, HTTPException, status, Depends, Query

from app.database import database
from app.auth_bearer import JWTBearer


load_dotenv()
router = APIRouter()


@router.get("/funding_data", tags=["data"])
async def get_impulse(interval: int = Query(7), token_data: Dict = Depends(JWTBearer())):
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))

    if interval == 1:
        time_gap = 60
    elif interval == 7:
        time_gap = 480
    else:
        time_gap = 1440

    sql_query = f"""
        WITH NumberedRows AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY stock_id ORDER BY funding_time) AS rn
            FROM 
                data_history.funding_data
        )
        SELECT 
            *
        FROM 
            NumberedRows
        WHERE 
            rn % {time_gap} = 1  
        ORDER BY 
            stock_id, 
            funding_time;
        """

    df = pd.read_sql_query(sql_query, conn)
    conn.close()

    funding_rates = df['funding_rate'].astype(float).tolist()

    positive_funding_rate = [rate for rate in funding_rates if rate > 0.01]  # положительные считаются только от 0.01%
    positive_funding_rate_quantity = len(positive_funding_rate)

    negative_funding_rate = [rate for rate in funding_rates if
                             0.005 != rate < 0.01]  # отрицательные должны быть меньше 0.01%, но не равны 0.005%
    negative_funding_rate_quantity = len(negative_funding_rate)

    neutral_funding_rate = [rate for rate in funding_rates if
                            rate == 0.01 or rate == 0.05]  # нейтральные равны либо 0.01%, либо 0.005%
    neutral_funding_rate_quantity = len(neutral_funding_rate)

    user_id = token_data.get("user_id")
    csv_file_path = f"dataframes/funding_data_{user_id}.csv"
    df.to_csv(csv_file_path, index=False)

    await database.execute(
        """
        INSERT INTO data_history.funding_data_history (user_id, created, positive_count, negative_count, neutral_count) 
        VALUES ($1, $2, $3, $4, $5);
        """, user_id, datetime.now(), positive_funding_rate_quantity, negative_funding_rate_quantity, neutral_funding_rate_quantity
    )

    return {"status": "success",
            "positive_quantity": positive_funding_rate_quantity,
            "negative_quantity": negative_funding_rate_quantity,
            "neutral_quantity": neutral_funding_rate_quantity,
            "positive_rate": positive_funding_rate,
            "negative_rate": negative_funding_rate,
            "neutral_rate": neutral_funding_rate
            }
