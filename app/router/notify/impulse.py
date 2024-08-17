from datetime import datetime
from typing import Dict

from dotenv import load_dotenv

from fastapi import APIRouter, HTTPException, status, Depends

from app.database import database
from app.auth_bearer import JWTBearer
from .schemas import Impulse


load_dotenv()
router = APIRouter()



@router.get("/get_impulse", tags=["notify"])
async def get_impulse(token_data: Dict = Depends(JWTBearer())):
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


@router.post("/set_impulse", tags=["notify"])
async def set_impulse(impulse_params: Impulse, token_data: Dict = Depends(JWTBearer())):
    status_to_add = await database.fetch(
        """
            WITH notification_count AS (
                SELECT COUNT(*) AS count
                FROM users.user_notification
                WHERE user_id = $1 AND notification_type = 'last_impulse'
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

    condition = f"{impulse_params.interval}:{impulse_params.percentage}"

    if status_to_add[0].get("allowed_to_add"):
        try:
            await database.execute(
                """
                INSERT INTO users.user_notification (user_id, notification_type, notify_time, condition, created, interval) 
                VALUES ($1, 'last_impulse', NULL, $2, $3, $4)
                """, token_data.get("user_id"), condition, datetime.now(), impulse_params.interval
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Error arose while adding new notification."
            )

    else:
        try:
            await database.execute(
                """
                UPDATE users.user_notification
                SET condition = $2, created = $3, interval = $4
                WHERE id = (
                    SELECT id
                    FROM users.user_notification
                    WHERE user_id = $1
                    ORDER BY created ASC
                    LIMIT 1
                );
                """, token_data.get("user_id"), condition, datetime.now(), impulse_params.interval
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Error arose while adding new notification."
            )

    return {"Success": True}