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
    print(update)

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    reply_markup = {
        "inline_keyboard": [[{
            "text": "go to game",
            "web_app": {"url": "https://756e-89-107-97-177.ngrok-free.app"}
        }]]
    }

    payload = {
        "chat_id": update.message.get('from').get('id'),
        "text": """🎉 Добро пожаловать в Smart Analytics!

Smart Analytics — это ваш персональный помощник в мире криптовалют и фондового рынка. Получайте актуальные данные, анализируйте рынок и принимайте взвешенные решения.

🔍 Исследуйте рынок: Отслеживайте изменения цен, активность торгов и рост активов в реальном времени.

📈 Анализируйте данные: Используйте эксклюзивные аналитические инструменты для глубокого понимания рыночных тенденций.

💼 Подписка: Откройте доступ к расширенным функциям и получайте максимум полезной информации.

Удачи в ваших инвестициях и успешных сделок! 🚀""",
        "reply_markup": reply_markup,
    }

    response = requests.post(url, json=payload)
    logging.info(response)
    print(response)
    return {"Status": "ok"}


# @router.on_event("startup")
# async def on_startup():
#     url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
#     payload = {"url": WEBHOOK_URL}
#     response = requests.post(url, json=payload)
#     if response.status_code != 200:
#         raise HTTPException(status_code=response.status_code, detail="Failed to set webhook")
#     logging.info(f"Webhook set: {WEBHOOK_URL}")
#     print(f"Webhook set: {WEBHOOK_URL}")
#
#
# @router.on_event("shutdown")
# async def on_shutdown():
#     url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook"
#     response = requests.post(url)
#     if response.status_code != 200:
#         raise HTTPException(status_code=response.status_code, detail="Failed to delete webhook")
#     logging.info("Webhook deleted")
