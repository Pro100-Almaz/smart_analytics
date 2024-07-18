import requests
from fastapi import APIRouter

router = APIRouter()
@router.get("/get_funding_data")
async def get_funding_data():
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    response = requests.get(url)
    if response.status_code == 200:
        funding_data = response.json()
        for i in funding_data:
            i['fundingRate'] = round(float(i['fundingRate']) * 100, 5)
        return funding_data
