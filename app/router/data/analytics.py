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
async def get_funding_history(token_data: Dict = Depends(JWTBearer())):
    data = await database.fetch(
        """
        SELECT created, positive_count, negative_count, neutral_count
        FROM data_history.funding_data_history
        WHERE user_id = $1
        """, token_data.get("user_id")
    )

    return {"status": "success", "data": data}