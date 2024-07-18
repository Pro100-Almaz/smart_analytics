# import requests
# from fastapi import HTTPException, APIRouter
#
#
# router = APIRouter()
#
#
# @router.get("/get_funding_data")
# def get_funding_data():
#     url = "https://fapi.binance.com/fapi/v1/fundingRate"
#     response = requests.get(url)
#     # TODO: make this function for providing the information
#     if response.status_code == 200:
#         pass
