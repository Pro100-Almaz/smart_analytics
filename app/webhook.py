from typing import Union

from fastapi import HTTPException, APIRouter, Request
from pydantic import BaseModel
from dotenv import load_dotenv

import os
import requests

from app.database import database
from i18n import i18n


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
    if not update.message.get("text", None):
        return {"Status": "ok"}

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    telegram_id = update.message.get("from").get("id")
    username = update.message.get("from").get("username", None)
    language_code = update.message.get("from").get("language_code", "en")
    first_name = update.message.get("from").get("first_name", None)
    last_name = update.message.get("from").get("last_name", None)
    is_tg_premium = update.message.get("from").get("is_premium", False)
    new_referral_link = "https://t.me/practically_bot?start=refId" + str(telegram_id)

    bot_return_text = i18n.get_string('bot.default_text', 'en')
    process_status = "success"

    if update.message.get("text").startswith("/start refId"):

        user_id = await database.fetchrow(
            """
            SELECT user_id
            FROM users."user"
            WHERE telegram_id = $1
            """, telegram_id
        )

        if user_id is None:
            ref_id = update.message.get("text").split(" ")[1][5:]

            referring_id = await database.fetchrow(
                """
                SELECT user_id
                FROM users."user"
                WHERE telegram_id = $1
                """, int(ref_id)
            )

            referring_id = int(referring_id.get("user_id"))

            try:
                result = await database.fetch(
                    """
                    INSERT INTO users.user (telegram_id, username, profile_photo, first_name, last_name, 
                    language_code, referral_link)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING user_id
                    """, telegram_id, username, None, first_name, last_name, language_code, new_referral_link
                )

                user_id = int(result[0].get('user_id'))

                await database.execute(
                    """
                    INSERT INTO users.referral_list (user_id, referred_id, requested, cash)
                    VALUES ($1, $2, $3, $4)
                    RETURNING user_id
                    """, user_id, referring_id, False, 0
                )

                return_text = i18n.get_string('bot.success_message', language_code).format(referred_id=telegram_id)
                bot_return_text = (i18n.get_string('bot.invited_client_welcome_text', language_code).
                                   format(user_nickname=username))

                payload = {
                    "chat_id": ref_id,
                    "text": return_text
                }

                response = requests.post(url, json=payload)
                print(response)

            except Exception as e:
                bot_return_text = i18n.get_string('bot.error_message', language_code)
                process_status = "error"

    elif update.message.get("text").startswith("/help"):
        bot_return_text = i18n.get_string('bot.help_message', 'en')
        process_status = "help"

    elif update.message.get("text").startswith("/start"):
        user_id = await database.fetchrow(
            """
            SELECT user_id
            FROM users."user"
            WHERE telegram_id = $1
            """, telegram_id
        )

        if user_id is None:
            try:
                await database.execute(
                    """
                    INSERT INTO users.user (telegram_id, username, profile_photo, first_name, last_name, 
                    language_code, referral_link)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING user_id
                    """, telegram_id, username, None, first_name, last_name, language_code, new_referral_link
                )

                bot_return_text = (i18n.get_string('bot.client_welcome_text', language_code).
                                   format(user_nickname=username))

            except Exception as e:
                bot_return_text = i18n.get_string('bot.error_message', language_code)
                process_status = "error"


    payload = {
        "chat_id": update.message.get('from').get('id'),
        "text": bot_return_text
    }

    if process_status == "success":
        reply_markup = {
            "inline_keyboard": [[{
                "text": "Lets trade!",
                "web_app": {"url": "https://smart-trade-kappa.vercel.app/"}
            }]]
        }

        payload["reply_markup"] = reply_markup

    response = requests.post(url, json=payload)

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
