from fastapi import APIRouter
from .funding_data import router as funding_data_router

router = APIRouter()

router.include_router(funding_data_router)