import hashlib
import json
import os
import hmac
import urllib.parse as urlparse
from datetime import datetime
from typing import Dict

from dotenv import load_dotenv

from fastapi import APIRouter, HTTPException, status, Depends

from .schemas import Authorization
from app.utils import create_access_token
from app.database import database
from ...auth_bearer import JWTBearer

load_dotenv()
router = APIRouter()


def create_hashed_link(user_id, username, telegram_id, hash_length=30):
    concatenated_string = f"{user_id}-{username}-{telegram_id}"

    hashed_value = hashlib.sha256(concatenated_string.encode()).hexdigest()
    truncated_hash = hashed_value[:hash_length]

    base_link = str(os.getenv('BASE_URL')) + "/referral_link/"
    hashed_link = f"{base_link}{urlparse.quote(truncated_hash)}"

    return hashed_link

def verify_telegram_web_app_data(telegram_init_data):
    init_data = urlparse.parse_qs(telegram_init_data)
    hash_value = init_data.get('hash', [None])[0]

    sorted_items = sorted((key, val[0]) for key, val in init_data.items() if key != 'hash')
    data_to_check = [f"{key}={value}" for key, value in sorted_items]

    secret = hmac.new(b"WebAppData", os.getenv('TELEGRAM_BOT_TOKEN').encode(), hashlib.sha256).digest()
    _hash = hmac.new(secret, "\n".join(data_to_check).encode(), hashlib.sha256).hexdigest()

    return _hash == hash_value, init_data.get("user", [None])[0]


@router.post("/login_user")
async def login_user(telegram_data: Authorization):
    data_from_telegram, user_tg_data = verify_telegram_web_app_data(telegram_data.data_check_string)

    if not data_from_telegram:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Not valid telegram data",
        )

    user_tg_data = json.loads(user_tg_data)

    user_data = await database.fetchrow(
        """
        UPDATE users.user
        SET last_login = current_timestamp, active = true
        WHERE telegram_id = $1 AND username = $2
        RETURNING user_id
        """, user_tg_data.get("id"), user_tg_data.get("username")
    )

    user_id = int(user_data.get('user_id'))

    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = create_access_token({"telegram_id": user_tg_data.get("id"), "username": user_tg_data.get("username"),
                                 "user_id": user_id})

    return {"token": token, "user_id": user_id, "Status": "success", "username": user_tg_data.get("username")}


@router.post("/get_referral_link")
async def login_user(token_data: Dict = Depends(JWTBearer())):
    referral_link = await database.fetchrow(
        """
        SELECT referral_link
        FROM users."user"
        WHERE user_id = $1
        """, token_data.get("user_id")
    )

    return {"status": "success", "link": referral_link.get("referral_link")}