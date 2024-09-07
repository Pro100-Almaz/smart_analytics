from fastapi import APIRouter

from .impulse import router as impulse_router
from .ticker_tracking import router as ticker_tracking_router


router = APIRouter()

router.include_router(impulse_router)
router.include_router(ticker_tracking_router)
