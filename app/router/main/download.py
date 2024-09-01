from fastapi import FastAPI, Depends, APIRouter, Query
from typing import Dict
from fastapi.responses import FileResponse
from app.auth_bearer import JWTBearer

from app.database import database

router = APIRouter()


# @router.get("/download-funding", tags=["default"])
# def download_funding(token_data: Dict = Depends(JWTBearer())):
#     user_id = token_data["user_id"]
#     csv_file_path = f"dataframes/funding_data_{user_id}.csv"
#     return FileResponse(path=csv_file_path, filename="data.csv", media_type='text/csv')
#
#
# @router.get("/download-growth", tags=["default"])
# async def download_growth(file_id: int = Query(), token_data: Dict = Depends(JWTBearer())):
#     if not file_id:
#         return {"Provide file id!"}
#
#     user_id = token_data["user_id"]
#
#     file_params = await database.fetchrow(
#         """
#         SELECT *
#         FROM data_history.growth_data_history
#         WHERE file_id = $1 AND user_id = $2;
#         """, file_id, user_id
#     )
#
#     date_param = file_params.get("date")
#     time_param = file_params.get("time")
#     file_name = file_params.get("file_name")
#
#     csv_file_path = f"dataframes/{user_id}/{date_param}/{time_param}/{file_name}"
#     return FileResponse(path=csv_file_path, filename=file_name, media_type='text/csv')
