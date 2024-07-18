from fastapi import FastAPI
# from app.database import database
from fastapi.middleware.cors import CORSMiddleware

from app.webhook import router as webhook_router
from app.router.main.funding_data import router as funding_data_router
app = FastAPI(
    title="Trading app"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(webhook_router, prefix="/webhook")
app.include_router(funding_data_router, prefix="/tickers")

# @app.on_event("startup")
# async def startup():
#     await database.connect()
#
# @app.on_event("shutdown")
# async def shutdown():
#     await database.disconnect()
#
