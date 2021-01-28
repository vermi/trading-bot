# Python Trading Bot (and accessories)

This is a small suite of tools for use in cloud-based day trading activities.

## Contents

* `nyseBackData.py`: a script to download historical data for NYSE ticker symbols.
* `getData.py`: a script that is meant to be run daily after market close. Updates price data.
* `tradingBot.py`: the actual trading bot script.

Each script also has its own requirements.txt. These are important for the cloud deployment.

## Algorithm

Currently using a 125-day momentum algorithm blatantly stolen from Clenow's Trading Evolved. This may change in the future depending on results.

## Initial Setup

### Alpaca Account Setup

1. Visit <https://alpaca.markets> and create an account. I recommend creating both a Paper account and a Live account. Use the Paper account for thorough testing before attempting live trades.
2. Under `Your API Keys`, click to generate a new API key. Make note of the secret, as it will only be displayed once.

### TD Ameritrade Developer Account Setup

1. Visit <https://developer.tdameritrade.com/apis> and register for an account.
2. Click on `My Apps` and then create a new app. Callback URL should be `http://localhost`
3. Note the Consumer Key for your app.

### Google Cloud Platform Setup

1. Create an account on Google Cloud Platform and set up a new project.
2. I recommend pinning the following to the Navigation Menu: Cloud Functions, Cloud Scheduler, BigQuery and Storage.
3. In BigQuery, create a dataset called `equity_data`. Also make a note of your database name
4. In Storage, upload a text file called `alpaca-key` containing your Alpaca credentials in the following format `<KEY ID>,<SECRET>`
5. In Storage, upload a text file called `td-key` containing your TD Ameritrade API key

## Deployment

### Deploy data script

* In Google Cloud Functions, create a new function with the following Configuration:

  * Function Name: `daily_equity_quotes_cf`
  * Region: Select your favorite
  * Trigger Type: Cloud Pub/Sub
  * Topic: (Create a Topic) -> `daily_equity_quotes_topic`
  * Advanced -> Memory Allocated: 1 GiB
  * Advanced -> Timeout: 520

* In the code section:

  * Runtime: Python 3.7
  * Entry point: `main`
  * main.py: Copy/paste the contents of `getData.py`
  * requirements.txt: Copy/paste the contents of `get_data_requirements.txt`

* Click Deploy, and wait for completion
* In Cloud Scheduler, create a new job with the following parameters:

  * Name: data_schedule_daily
  * Frequency: `0 17 * * 1-5`
  * Time Zone: America/EST
  * Target: Pub/Sub
  * Topic: `daily_equity_quotes_topic`
  * Payload: Enter anything you want here. It is required, but not used by the script.

* Once the job is created, click "Run Now" to execute the first data pull and create the table.

### Upload historical data

* Upload `back_data.csv` to your Storage bucket. (See [Usage](#nysebackdata.py) below.)
* Browse to your `equity_data` dataset in BigQuery.
* Click on Create Table
* Enter the following paramters:
  * Create table from: Google Cloud Storage
  * Select file from GCS bucket: (Navigate and select) -> `back_data.csv`
  * Table name: `daily_quote_data`
  * Auto detect: [X] Scheme and input parameters
  * Advanced -> Write Preference: Append to table
  * Advanced -> [X] Ignore unknown values
  * Advanced -> [X] Allow jagged rows
* Click Create Table to append your historical data.

### Deploy trading bot

* In Google Cloud Functions, create a new function with the following Configuration:

  * Function Name: `trading_bot_cf`
  * Region: Select your favorite
  * Trigger Type: Cloud Pub/Sub
  * Topic: (Create a Topic) -> `trading_bot_topic`
  * Advanced -> Memory Allocated: 4 GiB
  * Advanced -> Timeout: 520

* In the code section:

  * Runtime: Python 3.7
  * Entry point: `main`
  * main.py: Copy/paste the contents of `tradingBot.py`
  * requirements.txt: Copy/paste the contents of `trading_bot_requirements.txt`

* Click Deploy, and wait for completion
* In Cloud Scheduler, create a new job with the following parameters:

  * Name: data_schedule_daily
  * Frequency: Choose the best time for you. I am currently using `15 10 * * 1-5`
  * Time Zone: America/EST
  * Target: Pub/Sub
  * Topic: `trading_bot_topic`
  * Payload: Enter anything you want here. It is required, but not used by the script.

* Once the job is created, you can click "Run Now" to execute your first trade. See [Usage](#trading-bot) below for caveats.

## Usage

### Trading bot

In order to execute your first strategy trade, you must already have positive value open positions.

The bot will (probably) sell off some of your open positions in order to buy positions in the selected stocks.

You can adjust the momentum calculation range (default 125 days) and the momentum score threshold (default 40) to tweak how trades work. Support for multiple algorithms will come eventually.

### nyseBackData.py

```bash
Usage: nyseBackData.py [OPTIONS] START END

  A script to download historical NYSE price data. Exports to CSV.

  If --symbol is not specified, the script will download data for all NYSE
  ticker symbols.

Options:
  -p, --path PATH            Path to save exported CSV.  [default: .]
  -f, --freq [daily|weekly]  Price data frequency.  [default: daily]
  -s, --symbol TEXT          Specify which symbols to look up historical data.
                             This option may be passed multiple times.

  --help                     Show this message and exit.
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

This suite is proprietary and not licensed for commercial or private use.
