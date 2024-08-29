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


@router.get("/ticker_information")
async def get_funding_history(ticker: str = Query(max_length=50), token_data: Dict = Depends(JWTBearer())):
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



    return {"status": "success", "data": data}