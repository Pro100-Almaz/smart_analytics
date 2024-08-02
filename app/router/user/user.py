import hashlib
import os
import urllib.parse
from datetime import datetime

from dotenv import load_dotenv

from fastapi import APIRouter, HTTPException, status, Depends

from .schemas import User

from app.utils import create_access_token
from app.database import database, redis_database


load_dotenv()
router = APIRouter()


def create_hashed_link(user_id, username, telegram_id, hash_length=30):
    concatenated_string = f"{user_id}-{username}-{telegram_id}"

    hashed_value = hashlib.sha256(concatenated_string.encode()).hexdigest()
    truncated_hash = hashed_value[:hash_length]

    base_link = str(os.getenv('BASE_URL')) + "/referral_link/"
    hashed_link = f"{base_link}{urllib.parse.quote(truncated_hash)}"

    return hashed_link

@router.post("/login_user")
async def login_user(user: User):
    telegram_id = user.data.get("id")
    username = user.data.get("username")

    user_data = await database.fetchrow(
        """
        UPDATE users.user
        SET last_login = $3
        WHERE telegram_id = $1 AND user_name = $2
        RETURNING user_id
        """, telegram_id, username, datetime.now()
    )

    user_id = int(user_data.get('user_id'))

    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = create_access_token({"telegram_id": telegram_id, "username": username,
                                 "user_id": user_id})
    redis_database.set_user_token(user_id, token)

    return {"token": token, "user_id": user_id, "Status": "200", "username": username}
