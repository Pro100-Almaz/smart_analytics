from datetime import datetime
from typing import Dict

from dotenv import load_dotenv

from fastapi import APIRouter, HTTPException, status, Depends, Query

from app.database import database
from app.auth_bearer import JWTBearer


load_dotenv()
router = APIRouter()



@router.get("/gradation_growth", tags=["notify"])
async def get_impulse(interval: int = Query(5), sort_type: str = Query(None, max_length=50), token_data: Dict = Depends(JWTBearer())):
    impulses = await database.fetch(
        """
        SELECT *
        FROM users.user_notification
        WHERE user_id = $1 AND notification_type = 'last_impulse'
        """, token_data.get("user_id")
    )

    return impulses


@router.get("/get_impulse_history", tags=["notify"])
async def get_impulse(token_data: Dict = Depends(JWTBearer())):
    # impulses = await database.fetch(
    #     """
    #     SELECT *
    #     FROM users.user_notification
    #     WHERE user_id = $1 AND notification_type = 'last_impulse'
    #     """, token_data.get("user_id")
    # )

    return {}
