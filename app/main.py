from fastapi import FastAPI
from app.database import database
from fastapi.middleware.cors import CORSMiddleware

from app.webhook import router as webhook_router
from app.router.user import router as user_router

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(webhook_router, prefix="/webhook")
app.include_router(user_router, prefix="/user")


@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

