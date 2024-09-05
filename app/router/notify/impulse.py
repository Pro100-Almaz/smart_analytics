from datetime import datetime
from typing import Dict

from dotenv import load_dotenv

from fastapi import APIRouter, HTTPException, status, Depends, Query

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

    return {"status": status.HTTP_200_OK, "impulses": impulses}


@router.get("/get_impulse_history", tags=["notify"])
async def get_impulse(token_data: Dict = Depends(JWTBearer())):
    notifications_id = await database.fetch(
        """
        SELECT id
        FROM users.user_notification
        WHERE user_id = $1 AND notification_type = 'last_impulse'
        """, token_data.get("user_id")
    )

    notifications_merged = [not_id.get('id') for not_id in notifications_id]
    placeholders = ','.join(f"${i + 1}" for i in range(len(notifications_merged)))

    impulses_history = await database.fetch(
        f"""
        SELECT active_name, date, percent, day_percent
        FROM users.notification
        WHERE type IN ({placeholders})
        ORDER BY date DESC 
        LIMIT 10;
        """, *notifications_merged
    )

    return {"status": status.HTTP_200_OK, "impulses_history": impulses_history}


@router.delete("/delete_impulse", tags=["notify"])
async def delete_impulse(impulse_id: int = Query(None), token_data: Dict = Depends(JWTBearer())):
    if impulse_id is None:
        return {"status": "error", "message": "No impulse id"}

    try:
        await database.execute(
            """
            DELETE FROM users.user_notification
            WHERE user_id = $1 AND id = $2
            """, token_data.get("user_id"), impulse_id
        )
    except:
        return {"status": "error", "message": "Impulse not found"}

    return {"status": status.HTTP_200_OK, "message": "Impulse deleted"}


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

    condition = f"{impulse_params.interval}_min:{impulse_params.percentage}"

    if status_to_add[0].get("allowed_to_add"):
        try:
            await database.execute(
                """
                INSERT INTO users.user_notification (user_id, notification_type, notify_time, condition, created) 
                VALUES ($1, 'last_impulse', NULL, $2, $3)
                """, token_data.get("user_id"), condition, datetime.now()
            )
        except Exception as e:
            print(e)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Error arose while adding new notification. Condition 1"
            )

    else:
        try:
            await database.execute(
                """
                DELETE FROM users.user_notification WHERE id = (
                    SELECT id
                    FROM users.user_notification
                    WHERE user_id = $1
                    ORDER BY created ASC
                    LIMIT 1
                );
                """, token_data.get("user_id")
            )

            await database.execute(
                """
                INSERT INTO users.user_notification (user_id, notification_type, notify_time, condition, created) 
                VALUES ($1, 'last_impulse', NULL, $2, $3)
                """, token_data.get("user_id"), condition, datetime.now()
            )
        except Exception as e:
            print(e)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Error arose while adding new notification. Condition 2"
            )

    return {"Success": True}