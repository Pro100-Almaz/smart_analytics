from fastapi import APIRouter, Depends, Query
from typing import Dict
from pydantic import BaseModel
from dotenv import load_dotenv
from telegram import Bot

import os
import requests

from app.database import database
from i18n import i18n
from app.auth_bearer import JWTBearer


load_dotenv()

# Define your Telegram bot token
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
WEBHOOK_URL = os.getenv('BASE_URL') + "/webhook"
DEBUG = os.getenv('DEBUG', False)

router = APIRouter()
bot = Bot(token=TELEGRAM_BOT_TOKEN)


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

                await database.execute(
                    """
                    INSERT INTO users.premium (user_id, status, last_payment, discout) 
                    VALUES ($1, $2, NULL, NULL)
                    """, user_id, True
                )

                await database.execute(
                    """
                    INSERT INTO users.notification_settings (user_id)
                    VALUES ($1);
                    """, user_id
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
        print(user_id)

        if user_id is None:
            try:
                result = await database.execute(
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
                    INSERT INTO users.premium (user_id, status, last_payment, discout) 
                    VALUES ($1, $2, NULL, NULL)
                    """, user_id, True
                )

                await database.execute(
                    """
                    INSERT INTO users.notification_settings (user_id)
                    VALUES ($1);
                    """, user_id
                )

                bot_return_text = (i18n.get_string('bot.client_welcome_text', language_code).
                                   format(user_nickname=username))

            except Exception as e:
                print(e)
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


@router.get("/send_funding_data", tags=["data"])
async def get_funding_data_file(token_data: Dict = Depends(JWTBearer())):
    user_id = token_data["user_id"]
    telegram_id = token_data["telegram_id"]

    csv_file_path = f"dataframes/funding_data_{user_id}.csv"
    with open(csv_file_path, 'rb') as file:
        await bot.send_document(chat_id=telegram_id, document=file)

    return {"Status": "ok"}


@router.get("/send_growth_data", tags=["default"])
async def download_growth(file_id: int = Query(), token_data: Dict = Depends(JWTBearer())):
    if not file_id:
        return {"Provide file id!"}

    user_id = token_data["user_id"]

    file_params = await database.fetchrow(
        """
        SELECT *
        FROM data_history.growth_data_history
        WHERE file_id = $1 AND user_id = $2;
        """, file_id, user_id
    )

    date_param = file_params.get("date")
    time_param = file_params.get("time")
    file_name = file_params.get("file_name")

    csv_file_path = f"dataframes/{user_id}/{date_param}/{time_param}/{file_name}"
    telegram_id = token_data["telegram_id"]

    with open(csv_file_path, 'rb') as file:
        await bot.send_document(chat_id=telegram_id, document=file)

    return {"Status": "ok"}


# @router.on_event("startup")
# async def on_startup():
#     if DEBUG:
#         url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
#         payload = {"url": WEBHOOK_URL}
#         response = requests.post(url, json=payload)
#         if response.status_code != 200:
#             raise HTTPException(status_code=response.status_code, detail="Failed to set webhook")
#         print(f"Webhook set: {WEBHOOK_URL}")
#         logger.info("Webhook set!")
#     else:
#         print("Production server!")


# @router.on_event("shutdown")
# async def on_shutdown():
#     if DEBUG:
#         url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook"
#         response = requests.post(url)
#         if response.status_code != 200:
#             raise HTTPException(status_code=response.status_code, detail="Failed to delete webhook")
#         logger.info("Webhook dropped!")
