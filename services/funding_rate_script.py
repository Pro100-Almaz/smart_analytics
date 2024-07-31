import pandas as pd
import numpy as np
import requests


def get_funding_data():
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    response = requests.get(url)
    response.raise_for_status()

    funding_data = pd.DataFrame(response.json())
    funding_data['fundingRate'] = np.round(funding_data['fundingRate'].astype(float) * 100, 5)
    funding_data = funding_data.sort_values(['fundingRate', 'symbol']).reset_index(drop=True)
    return funding_data


data = get_funding_data()
top_5_negative_funding = data.head(5)['symbol'].tolist()
top_5_positive_funding = data.tail(5)['symbol'].tolist()
