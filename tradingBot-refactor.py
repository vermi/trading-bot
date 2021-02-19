from datetime import datetime

import alpaca_trade_api as tradeapi
import numpy as np
import pandas as pd
import pytz
import requests
from google.cloud import bigquery, storage
from pypfopt import expected_returns, risk_models
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices
from pypfopt.efficient_frontier import EfficientFrontier
from scipy import stats

# Globals
base_url = "https://paper-api.alpaca.markets"
today = datetime.today().astimezone(pytz.timezone("America/New_York"))
today_fmt = today.strftime("%Y-%m-%d")


def fetch_api_key(bucket_name="fair-sandbox", file_name="td-key") -> str:
    """
    Retrieves the TDA API key from Google Cloud Storage

        Parameters:
            bucket_name (str): the name of the bucket containing the key file
            file_name (str): the name of the file containing the key

        Returns:
            api_key (str): The TDA API key
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    api_key = blob.download_as_string()

    return api_key


def check_open() -> bool:
    """
    Returns the market open status for the day.

        Returns:
            is_open (bool): Boolean representing open status
    """
    # Call the TDA Hours endpoint for equities to see if it is open
    market_url = "https://api.tdameritrade.com/v1/marketdata/EQUITY/hours"

    params = {
        "apikey": fetch_api_key(),
        "date": today_fmt,
    }

    request = requests.get(url=market_url, params=params).json()
    is_open = request["equity"]["EQ"]["isOpen"]

    return is_open


def trade():
    pass


def main(event, context) -> str:
    """
    This script is used to optimize the portfolio and execute trades.

        Returns:
            status (str): A status message for the Google Cloud Platform run log
    """
    try:
        if check_open():
            trade()
            return "Trading complete."
        else:
            return "No trade: markets closed."
    except Exception:
        pass  # TODO: implement


if __name__ == "__main__":
    main(None, None)
