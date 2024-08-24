from fastapi import APIRouter

from .download import router as default_router


router = APIRouter()

router.include_router(default_router)