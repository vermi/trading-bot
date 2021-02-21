from datetime import datetime

import alpaca_trade_api as tradeapi
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
import pytz
import requests
from google.cloud import bigquery, storage
from pypfopt import expected_returns, risk_models
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices
from pypfopt.efficient_frontier import EfficientFrontier
from scipy import stats

# Globals
DEBUG = True
alpaca_url = "https://paper-api.alpaca.markets"
bucket_name = "fair-sandbox"  # Google Cloud Storage bucket name
db_name = "fair-sandbox-132013"  # BigQuery database name
today = datetime.today().astimezone(pytz.timezone("America/New_York"))
today_fmt = today.strftime("%Y-%m-%d")


class Strategy:
    """
    A generic class to represent a trading strategy. For future use.

        Attributes:
            name (str): The strategy name

        Methods:
            get_buy_list(): Gets a list of buy stocks
            prepare_df(): Prepares the DataFrame
    """

    def __init__(self):
        self.name = None

    def get_buy_list():
        pass

    def prepare_df():
        pass


class Momentum(Strategy):
    """
    A class to represent the momentum trading strategy.

        Attributes:
            name (str): The strategy name
            window (int): The number of days to calculate momentum
            minimum (int): The minimum momentum score to consider buying
            portfiolo_size (int): The target number of stocks to hold

        Methods:
            __score(ts): Computes the momentum score for a ticker symbol
            get_buy_list(df, date, portfolio_size, cash): Computes the list of buy stocks
            prepare_df(df): Prepares the historical DataFrame by calculating momentum
    """

    def __init__(self, window: int = 125, minimum: int = 40, portfolio_size: int = 10):
        """Intializes Momentum with passed or default values."""
        self.name = "momentum"
        self.window = window
        self.minimum = minimum
        self.portfolio_size = portfolio_size

    def __score(self, ts: pd.Series) -> int:
        """
        Calculates the moment score of the selected ticker symbol. For use with pandas apply().

            Parameters:
                ts (Series): A pandas Series containing info about the ticker symbol

            Returns:
                score (int): The calculated momentum score of the ticker symbol
        """
        x = np.arange(len(ts))
        log_ts = np.log(ts)
        regress = stats.linregress(x, log_ts)
        annualized_slope = (np.power(np.exp(regress[0]), 252) - 1) * 100
        score = annualized_slope * (regress[2] ** 2)
        return score

    def get_buy_list(self, df: DataFrame, date: datetime, cash: float) -> DataFrame:
        """
        Calculates the buy stocks based on desired portfolio size, and available equity.

            Parameters:
                df (DataFrame): A dataframe containing momentum-scored and filtered ticker symbols.
                date (datetime): A datetime representing the most recent available market data.
                cash (float): The total available equity.

            Returns:
                df_buy (DataFrame): a DataFrame containing the optimized buy list.
        """
        # Filter the df to get the top momentum stocks for the latest day based on desired portfolio size
        df_top_m = df.loc[df["date"] == pd.to_datetime(date)]
        df_top_m = df_top_m.sort_values(by="momentum", ascending=False).head(
            self.portfolio_size
        )

        # Set the universe to the top momentum stocks for the period
        universe = df_top_m["symbol"].tolist()

        # Create a df with just the stocks from the universe
        df_u = df.loc[df["symbol"].isin(universe)]

        # Create the portfolio
        # Pivot to format for the optimization library
        df_u = df_u.pivot_table(
            index="date", columns="symbol", values="close", aggfunc="sum"
        )

        # Calculate expected returns and sample covariance
        mu = expected_returns.mean_historical_return(df_u)
        S = risk_models.sample_cov(df_u)

        # Optimise the portfolio for maximal Sharpe ratio
        ef = EfficientFrontier(mu, S)
        weights = (
            ef.max_sharpe()
        )  # Ignore the fact that this variable isn't referenced. EfficientFrontier is weird.
        cleaned_weights = ef.clean_weights()

        # Allocate
        latest_prices = get_latest_prices(df_u)
        da = DiscreteAllocation(
            cleaned_weights, latest_prices, total_portfolio_value=cash
        )
        allocation = da.lp_portfolio()[0]

        # Put the stocks and the number of shares from the portfolio into a df
        symbol_list = []
        num_shares_list = []

        for symbol, num_shares in allocation.items():
            symbol_list.append(symbol)
            num_shares_list.append(num_shares)

        # Now that we have the stocks we want to buy we filter the df for those
        df_buy = df.loc[df["symbol"].isin(symbol_list)]

        # Filter for the period to get the closing price
        df_buy = df_buy.loc[df_buy["date"] == date].sort_values(by="symbol")

        # Add in the qty that was allocated to each stock
        df_buy["qty"] = num_shares_list

        # Calculate the amount we own for each stock
        df_buy["amount_held"] = df_buy["close"] * df_buy["qty"]
        df_buy = df_buy.loc[df_buy["qty"] != 0]
        return df_buy

    def prepare_df(self, df: DataFrame) -> DataFrame:
        """
        Prepares the DataFrame by calculating momentum.

            Parameters:
                df (DataFrame): A DataFrame containing historical market data

            Returns:
                df (DataFrame): A DataFrame containing prepared historical market data
        """
        df["momentum"] = (
            df.groupby("symbol")["close"]
            .rolling(self.window, min_periods=self.minimum)
            .apply(self.__score)
            .reset_index(level=0, drop=True)
        )

        return df


def fetch_api_key(vendor: str, bucket_name: str) -> str:
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

    api_key, _ = fetch_api_key("tda", bucket_name)
    params = {
        "apikey": api_key,
        "date": today_fmt,
    }

    request = requests.get(url=market_url, params=params).json()
    is_open = request["equity"]["EQ"]["isOpen"]

    return is_open


def get_sell_data(
    df: DataFrame, df_pf: DataFrame, sell_list: list, date: datetime
) -> DataFrame:
    """
    Compares the existing portfolio to the buy list to determine which stocks to sell.

        Parameters:
            df (DataFrame): The DataFrame containing momentum scored stock data
            df_pf (DataFrame): A DataFrame containing current portfolio data
            sell_list (list): A list of symbols to divest
            date (datetime): A datetime representing the most recent market data

        Returns:
            df_sell (DataFrame): A DataFrame containing market data for sell stocks
    """
    # Get the current prices and the number of shares to sell
    df_sell_price = df.loc[df["date"] == pd.to_datetime(date)]

    # Filter
    df_sell_price = df_sell_price.loc[df_sell_price["symbol"].isin(sell_list)]

    # Check to see if there are any stocks in the current ones to buy
    # that are not in the current portfolio. It's possible there may not be any
    if df_sell_price.shape[0] > 0:
        df_sell_price = df_sell_price[["symbol", "close"]]

        # Merge with the current pf to get the number of shares we bought initially
        # so we know how many to sell
        df_buy_shares = df_pf[["symbol", "qty"]]

        df_sell = pd.merge(df_sell_price, df_buy_shares, on="symbol", how="left")

    else:
        df_sell = None

    return df_sell


def diff_stocks(df_sell: DataFrame, df_pf: DataFrame, df_buy: DataFrame) -> DataFrame:
    """
    Compares the sell, buy, and current portfolio stocks to finalize sell order.

        Parameters:
            df_sell (DataFrame): A DataFrame containing sell stock data
            df_pf (DataFrame): A DataFrame containing the current portfolio data
            df_buy (DataFrame): A DataFrame containing the buy stock data

        Returns:
            df_sell_final (DataFrame): A DataFrame containing the finalized sell order data
    """
    df_stocks_held_prev = df_pf[["symbol", "qty"]]
    df_stocks_held_curr = df_buy[["symbol", "qty", "close"]]

    # Inner merge to get the stocks that are the same week to week
    df_stock_diff = pd.merge(
        df_stocks_held_curr, df_stocks_held_prev, on="symbol", how="inner"
    )

    # Check to make sure not all of the stocks are different compared to what we have in the pf
    if df_stock_diff.shape[0] > 0:
        # Calculate any difference in positions based on the new pf
        df_stock_diff["share_amt_change"] = (
            df_stock_diff["qty_x"] - df_stock_diff["qty_y"]
        )

        # Create df with the share difference and current closing price
        df_stock_diff = df_stock_diff[["symbol", "share_amt_change", "close"]]

        # If there's less shares compared to last week for the stocks that
        # are still in our portfolio, sell those shares
        df_stock_diff_sale = df_stock_diff.loc[df_stock_diff["share_amt_change"] < 0]

        # If there are stocks whose qty decreased,
        # add the df with the stocks that dropped out of the pf
        if df_stock_diff_sale.shape[0] > 0:
            if df_sell is not None:
                df_sell_final = pd.concat([df_sell, df_stock_diff_sale], sort=True)
                # Fill in NaNs in the share amount change column with
                # the qty of the stocks no longer in the pf, then drop the qty columns
                df_sell_final["share_amt_change"] = df_sell_final[
                    "share_amt_change"
                ].fillna(df_sell_final["qty"])
                df_sell_final = df_sell_final.drop(["qty"], 1)
                # Turn the negative numbers into positive for the order
                df_sell_final["share_amt_change"] = np.abs(
                    df_sell_final["share_amt_change"]
                )
                df_sell_final.columns = df_sell_final.columns.str.replace(
                    "share_amt_change", "qty"
                )
            else:
                df_sell_final = df_stock_diff_sale
                # Turn the negative numbers into positive for the order
                df_sell_final["share_amt_change"] = np.abs(
                    df_sell_final["share_amt_change"]
                )
                df_sell_final.columns = df_sell_final.columns.str.replace(
                    "share_amt_change", "qty"
                )
        else:
            df_sell_final = None
    else:
        df_sell_final = df_stocks_held_curr

    return df_sell_final


def get_buy_data(df_pf: DataFrame, df_buy: DataFrame) -> DataFrame:
    """
    Compares the current portfolio and the buy list to finalize buy order.

        Parameters:
            df_pf (DataFrame): A DataFrame containing current portfolio data
            df_buy (DataFrame): A DataFrame containing buy stock data

        Returns:
            df_buy_new (DataFrame): A DataFrame containing finalized buy order data
    """
    # Left merge to get any new stocks or see if they changed qty
    df_buy_new = pd.merge(df_buy, df_pf, on="symbol", how="left")

    # Get the qty we need to increase our positions by
    df_buy_new = df_buy_new.fillna(0)
    df_buy_new["qty_new"] = df_buy_new["qty_x"] - df_buy_new["qty_y"]

    # Filter for only shares that increased
    df_buy_new = df_buy_new.loc[df_buy_new["qty_new"] > 0]
    if df_buy_new.shape[0] > 0:
        df_buy_new = df_buy_new[["symbol", "qty_new"]]
        df_buy_new = df_buy_new.rename(columns={"qty_new": "qty"})
    else:
        df_buy_new = None

    return df_buy_new


def get_hist_data() -> DataFrame:
    """
    Fetches historical market data from BigQuery and normalizes.

        Returns:
            df (DataFrame): A DataFrame containing normalized historical data.
    """
    # Establish BigQuery connection
    client = bigquery.Client()
    query_str = "SELECT symbol, closePrice, date FROM `{0}.equity_data.daily_quote_data`".format(
        db_name
    )

    # Execute query and build DataFrame
    df = client.query(query_str).to_dataframe()

    # Normalize data for processing
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(by="date").reset_index(drop=True)
    df = df.rename(columns={"closePrice": "close"})

    return df


def log_positions(positions: list) -> None:
    """
    Sends data regarding current positions to BigQuery for later reference.

        Parameters:
            positions (list): The list of positions to log.
    """
    client = bigquery.Client()

    symbol, qty, market_value = [], [], []

    for p in positions:
        symbol.append(p.symbol)
        qty.append(int(p.qty))

    # New position df
    position_df = pd.DataFrame({"symbol": symbol, "qty": qty})

    # Add the current date and other info into the portfolio df for logging
    position_df["date"] = pd.to_datetime(today_fmt)
    position_df["strat"] = "momentum_strat_1"

    # Add the new pf to BQ
    # Format date to match schema
    position_df["date"] = position_df["date"].dt.date

    # Append it to the anomaly table
    dataset_id = "equity_data"
    table_id = "strategy_log"

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.ignore_unknown_values = True

    job = client.load_table_from_dataframe(
        position_df, table_ref, location="US", job_config=job_config
    )

    job.result()


def trade() -> None:
    df = get_hist_data()
    current_data_date = df[
        "date"
    ].max()  # Find the most recent date for which we have data.

    # Set up Alpaca connection for trading
    key_id, secret_key = fetch_api_key("alpaca", bucket_name)
    api = tradeapi.REST(key_id, secret_key, alpaca_url, "v2")

    # Get list of current positions
    positions = api.list_positions()

    # Convert positions to DataFrame for processing.
    symbol, qty, market_value = [], [], []
    for p in positions:
        symbol.append(p.symbol)
        qty.append(int(p.qty))
        market_value.append(float(p.market_value))
    df_pf = pd.DataFrame({"symbol": symbol, "qty": qty, "market_value": market_value})
    if DEBUG:
        print("Current Portfolio:\n", df_pf)

    # Get current equity
    account = api.get_account()
    portfolio_value = float(account.equity)

    # Define trading strategy -- for now we only support Momentum
    strategy = Momentum()

    # Apply momentum strategy to historical data
    df = strategy.prepare_df(df)

    # Generate a buy list
    df_buy = strategy.get_buy_list(df, current_data_date, portfolio_value)

    # Figure out sell stocks and create DataFrame
    sell_list = list(set(df_pf["symbol"].tolist()) - set(df_buy["symbol"].to_list()))
    df_sell = get_sell_data(df, df_pf, sell_list, current_data_date)

    # Finalize and execute sell order
    df_sell = diff_stocks(df_sell, df_pf, df_buy)
    if df_sell is not None:
        if DEBUG:
            print("Sell Order:\n", df_sell)
        else:
            symbol_list = df_sell["symbol"].tolist()
            qty_list = df_sell["qty"].tolist()
            try:
                for symbol, qty in list(zip(symbol_list, qty_list)):
                    api.submit_order(
                        symbol=symbol,
                        qty=qty,
                        side="sell",
                        type="market",
                        time_in_force="day",
                    )
            except Exception:
                pass  # TODO: Should probably handle this more gracefully

    # Finalize and execute buy order
    df_buy = get_buy_data(df_pf, df_buy)
    if df_buy is not None:
        if DEBUG:
            print("Buy Order:\n", df_buy)
        else:
            symbol_list = df_buy["symbol"].tolist()
            qty_list = df_buy["qty"].tolist()
            try:
                for symbol, qty in list(zip(symbol_list, qty_list)):
                    api.submit_order(
                        symbol=symbol,
                        qty=qty,
                        side="buy",
                        type="market",
                        time_in_force="day",
                    )
            except Exception:
                pass  # TODO: Handle this more gracefully also

    # Log new positions to BigQuery for future reference
    if not DEBUG:
        positions = api.list_positions()
        log_positions(positions)


def main(event, context) -> str:
    """
    This script is used to optimize the portfolio and execute trades.

        Returns:
            status (str): A status message for the Google Cloud Platform run log
    """
    try:
        if DEBUG:
            trade()
            print("Trading complete.")
        elif check_open():
            trade()
            return "Trading complete."
        else:
            return "No trade: markets closed."
    except ValueError as verr:
        if DEBUG:
            print("ValueError: {0}".format(verr))
        return "ValueError: {0}".format(verr)
    except Exception as err:
        if DEBUG:
            print("Exception: {0}".format(err))
        return "Exception: {0}".format(err)  # TODO: Implement something better


if __name__ == "__main__":
    main(None, None)
