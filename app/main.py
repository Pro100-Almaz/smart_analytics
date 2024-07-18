from sys import prefix

from fastapi import FastAPI
# from app.database import database
from fastapi.middleware.cors import CORSMiddleware

from app.webhook import router as webhook_router
<<<<<<< HEAD
from app.router.user import router as user_router

app = FastAPI()
=======
from app.router.main.funding_data import router as funding_data_router
app = FastAPI(
    title="Trading app"
)
>>>>>>> b43732551ca0ce9dd01190b3870d5ef6c78133a5

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(webhook_router, prefix="/webhook")
<<<<<<< HEAD
app.include_router(user_router, prefix="/user")
=======
app.include_router(funding_data_router, prefix="/tickers")
>>>>>>> b43732551ca0ce9dd01190b3870d5ef6c78133a5

# @app.on_event("startup")
# async def startup():
#     await database.connect()
#
# @app.on_event("shutdown")
# async def shutdown():
#     await database.disconnect()
#
