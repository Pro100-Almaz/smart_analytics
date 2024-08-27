from fastapi import APIRouter

from .gradation import router as gradation_router
from .funding_data import router as funding_data_router
from .analytics import router as analytics_router


router = APIRouter()

router.include_router(gradation_router)
router.include_router(funding_data_router)
router.include_router(analytics_router, prefix="/analytics", tags=["data"])