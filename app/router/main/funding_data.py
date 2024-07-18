import pandas as pd
import numpy as np
import requests
from fastapi import HTTPException, APIRouter


router = APIRouter()


@router.get("/get_funding_data")
def get_funding_data():
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    response = requests.get(url)
    if response.status_code == 200:
        funding_data = pd.DataFrame(response.json())
        funding_data['fundingRate'] = np.round(funding_data.fundingRate.astype(float) * 100, 5)
        return funding_data.to_dict(orient="records")

