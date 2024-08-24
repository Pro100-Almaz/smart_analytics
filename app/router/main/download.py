from fastapi import FastAPI, Depends, APIRouter
from typing import Dict
from fastapi.responses import FileResponse
from app.auth_bearer import JWTBearer

router = APIRouter()


@router.get("/download-csv", tags=["default"])
def download_csv(token_data: Dict = Depends(JWTBearer())):
    user_id = token_data["user_id"]
    csv_file_path = f"dataframes/funding_data_{user_id}.csv"
    return FileResponse(path=csv_file_path, filename="data.csv", media_type='text/csv')
