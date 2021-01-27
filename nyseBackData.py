#!/usr/bin/env python3
import datetime
import string
import time

import click
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from pandas.core.frame import DataFrame
from progress.bar import Bar
from yahoofinancials import YahooFinancials as yf


def validate_date(ctx, param, value):
    try:
        datetime.datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        raise click.BadParameter("Dates must be in the format YYYY-MM-DD.")


def fetch_symbols() -> list:
    # Get a current list of all the stock symbols for the NYSE
    alpha = list(string.ascii_uppercase)

    symbols = []

    bar = Bar("Building symbol list", max=26)
    for each in alpha:
        url = "http://eoddata.com/stocklist/NYSE/{}.htm".format(each)
        resp = requests.get(url)
        site = resp.content
        soup = BeautifulSoup(site, "html.parser")
        table = soup.find("table", {"class": "quotes"})
        for row in table.findAll("tr")[1:]:
            symbols.append(row.findAll("td")[0].text.rstrip())
        bar.next()
    bar.finish()

    # Remove the extra letters on the end
    symbols_clean = []

    for each in symbols:
        each = each.replace(".", "-")
        symbols_clean.append((each.split("-")[0]))

    return symbols_clean


def fetch_history(symbols, start_date, end_date, freq) -> list:
    data_list = []

    print("Fetching {} price history from {} to {}".format(freq, start_date, end_date))
    bar = Bar("Fetching...", max=len(symbols))
    for s in symbols:
        data = yf(s)
        hist = data.get_historical_price_data(start_date, end_date, freq)
        data_list.append(hist)
        time.sleep(0.5)
        bar.next()
    bar.finish()

    return data_list


def parse_data(data) -> DataFrame:
    df = pd.DataFrame(
        {
            "symbol": [],
            "openPrice": [],
            "highPrice": [],
            "lowPrice": [],
            "closePrice": [],
            "date": [],
        }
    )

    # Iterate through JSON responses and collect relevant data
    bar = Bar("Parsing data", max=len(data))
    for entry in data:
        try:
            # Fetch the ticker symbol of this entry
            sym = list(entry.keys())[0]

            # Parse data and append to dataframe
            if entry[sym]["prices"]:
                for row in entry[sym]["prices"]:
                    df.loc[len(df)] = [
                        sym,
                        row["open"] if row["open"] else np.NaN,
                        row["high"] if row["high"] else np.NaN,
                        row["low"] if row["low"] else np.NaN,
                        row["close"] if row["close"] else np.NaN,
                        row["formatted_date"]
                        if row["formatted_date"]
                        else "1970-01-01",
                    ]

        except KeyError:
            pass
        except TypeError:
            pass
        bar.next()
    bar.finish()

    return df


def export_data(data, path):
    if path is None:
        path = r"."
    path = path + "/back_data.csv"

    # Export dataframe to CSV
    try:
        data.to_csv(path)
        print("CSV file saved successfully.")

    except IOError as e:
        print(e)


@click.command()
@click.argument("start", required=True, callback=validate_date)
@click.argument("end", required=True, callback=validate_date)
@click.option(
    "-p",
    "--path",
    default=r".",
    help="Path to save exported CSV.",
    type=click.Path(exists=True),
    show_default=True,
)
@click.option(
    "-f",
    "--freq",
    default="daily",
    type=click.Choice(["daily", "weekly"], case_sensitive=False),
    show_default=True,
    help="Price data frequency.",
)
@click.option(
    "-s",
    "--symbol",
    multiple=True,
    help="Specify which symbols to look up historical data. This option may be passed multiple times.",
)
def main(**kwargs):
    """
        A script to download historical NYSE price data. Exports to CSV.

        If --symbol is not specified, the script will download data for all NYSE ticker symbols.
    """

    if not kwargs["symbol"]:
        print("Fetching data for all available NYSE ticker symbols.")
        symbols = fetch_symbols()
    else:
        symbols = kwargs["symbol"]
        print("Fetching data for the following symbols: {}".format(symbols))

    data = fetch_history(symbols, kwargs["start"], kwargs["end"], kwargs["freq"])
    data = parse_data(data)
    export_data(data, kwargs["path"])


if __name__ == "__main__":
    main()
