from fastapi import APIRouter

from .impulse import router as impulse_router


router = APIRouter()

router.include_router(impulse_router)