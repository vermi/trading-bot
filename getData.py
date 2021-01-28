import string
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery, storage

DEBUG = False
# Cloud functions use UTC so we convert to EST
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


def request_quote(symbol, api_key) -> pd.DataFrame:
    """
    Retrieves quote data for a list of symbols via the TDA API.

        Parameters:
            symbol (list): A list of symbols to quote
            api_key (str): The TDA API credentials

        Returns:
            df_quotes (pd.DataFrame): A DataFrame containing the requested quotes.
    """
    url = r"https://api.tdameritrade.com/v1/marketdata/quotes"

    params = {
        "apikey": api_key,
        "symbol": symbol,
    }

    request = requests.get(url=url, params=params).json()
    df_quotes = pd.DataFrame.from_dict(request, orient="index").reset_index(drop=True)

    return df_quotes


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


def chunks(full_list, n):
    """
    Breaks a list down into manageable chunks.

        Parameters:
            full_list (list): the list that needs to be chunked
            n (int): max number of items per chunk

        Returns:
            chunk_list (list): a list of lists containing n items each
    """
    n = max(1, n)
    chunk_list = list((full_list[i : (i + n)] for i in range(0, len(full_list), n)))

    return chunk_list


def fetch_symbols() -> list:
    """
    Retrieves the NYSE symbols currently available for trade.

        Returns:
            symbols_clean (list): A list of symbols in a readable format
    """
    alpha = list(string.ascii_uppercase)

    symbols = []

    # Loop through the letters in the alphabet to get the stocks on each page
    # from the table and store them in a list
    for each in alpha:
        url = "http://eoddata.com/stocklist/NYSE/{}.htm".format(each)
        soup = BeautifulSoup(requests.get(url).content, "html.parser")
        table = soup.find("table", {"class": "quotes"})
        for row in table.findAll("tr")[1:]:
            symbols.append(row.findAll("td")[0].text.rstrip())

    # Remove any trailing letters or punctuation
    symbols_clean = []

    for each in symbols:
        each = each.replace(".", "-")
        symbols_clean.append((each.split("-")[0]))

    # use set() to remove duplicates
    return list(set(symbols_clean))


def get_data() -> pd.DataFrame:
    """
    Gathers quotes for all symbols and concatenates into a single DataFrame.

        Returns:
            df (pd.DataFrame): The aggregated quote data
    """
    symbols = fetch_symbols()

    # The TD Ameritrade api has a limit to the number of symbols you can get data for
    # in a single call so we chunk the list into 200 symbols at a time
    symbols_chunked = chunks(symbols, 200)

    # Iterate through symbols and request market data.
    df = pd.concat(
        [request_quote(sym, fetch_api_key()) for sym in symbols_chunked], sort=False,
    )

    return df


def load_data(df, dataset="equity_data", table="daily_quote_data"):
    """
    Loads quote data into BigQuery or saves as CSV (debug mode).

        Parameters:
            df (pd.DataFrame): the DataFrame containing quote data to load
            dataset (str): the BigQuery dataset
            table (str): the BigQuery table to append data
    """
    # Add the date and format for BigQuery
    df["date"] = pd.to_datetime(today_fmt)
    df["date"] = df["date"].dt.date
    df["divDate"] = pd.to_datetime(df["divDate"])
    df["divDate"] = df["divDate"].dt.date
    df["divDate"] = df["divDate"].fillna(np.nan)

    # Remove anything without a price
    df = df.loc[df["bidPrice"] > 0]

    # Rename columns and format for BQ (can't start with a number)
    df = df.rename(columns={"52WkHigh": "_52WkHigh", "52WkLow": "_52WkLow"})

    if not DEBUG:
        # Add to BigQuery
        client = bigquery.Client()

        dataset_ref = client.dataset(dataset)
        table_ref = dataset_ref.table(table)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True
        job_config.ignore_unknown_values = True
        job = client.load_table_from_dataframe(
            df, table_ref, location="US", job_config=job_config
        )

        job.result()
    else:
        # Otherwise save to CSV for testing purposes
        print("Debug mode. Saving to CSV.")
        df.to_csv(r"./debug-data.csv")


def main(event, context) -> str:
    """
    This script is used to fetch daily price data for all NYSE ticker symbols.

        Returns:
            status (str): A status message for the Google Cloud Platform run log
    """
    try:
        if check_open():
            load_data(get_data())
            return "Data load completed successfully."
        else:
            return "No data loaded: markets closed."
    except KeyError:
        return "No data loaded: markets closed."


if __name__ == "__main__":
    main(None, None)
