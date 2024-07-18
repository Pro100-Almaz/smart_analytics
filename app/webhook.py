from typing import Union

from fastapi import HTTPException, APIRouter, Request
from pydantic import BaseModel
from dotenv import load_dotenv

import os
import logging
import requests

load_dotenv()

# Define your Telegram bot token
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
WEBHOOK_URL = os.getenv('BASE_URL') + "/webhook"

router = APIRouter()


class Update(BaseModel):
    update_id: int
    message: dict


@router.post(f"")
async def webhook(update: Update):
    logging.info(update)

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    reply_markup = {
        "inline_keyboard": [[{
            "text": "go to game",
            "web_app": {"url": ""}
        }]]
    }

    payload = {
        "chat_id": update.message.get('from').get('id'),
        "text": """
            """,
        "reply_markup": reply_markup,
    }

    response = requests.post(url, json=payload)
    logging.info(response)
    return {"Status": "ok"}


@router.on_event("startup")
async def on_startup():
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
    payload = {"url": WEBHOOK_URL}
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to set webhook")
    logging.info(f"Webhook set: {WEBHOOK_URL}")
    print(f"Webhook set: {WEBHOOK_URL}")


@router.on_event("shutdown")
async def on_shutdown():
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook"
    response = requests.post(url)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Failed to delete webhook")
    logging.info("Webhook deleted")
