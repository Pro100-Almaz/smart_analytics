from fastapi import APIRouter

from .gradation import router as gradation_router


router = APIRouter()

router.include_router(gradation_router)