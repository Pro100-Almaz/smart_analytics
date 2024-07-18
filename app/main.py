import uvicorn
from fastapi import FastAPI
from app.database import database
from fastapi.middleware.cors import CORSMiddleware

from app.webhook import router as webhook_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(webhook_router, prefix="/webhook")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)