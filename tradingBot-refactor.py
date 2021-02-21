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


def fetch_api_key(vendor, bucket_name="fair-sandbox") -> str:
    """
    Retrieves the appropriate API key from Google Cloud Storage

        Parameters:
            vendor (str): either "tda" or "alpaca"
            bucket_name (str): the name of the bucket containing the key file

        Returns:
            api_key (tuple): The requested API key
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    if vendor == "tda":
        blob = bucket.blob("tda-key")
        api_key = (blob.download_as_string(), None)

    elif vendor == "alpaca":
        blob = bucket.blob("alpaca-key")
        api_key = tuple(blob.download_as_string().decode().split(","))

    else:
        raise ValueError("Invalid vendor specified.")

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
        "apikey": fetch_api_key("tda"),
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
