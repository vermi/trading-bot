# Python Trading Bot (and accessories)

This is a small suite of tools for use in cloud-based day trading activities.

## Contents

* nyseBackData.py: a script to download historical data for NYSE ticker symbols.
* getData.py: a script that is meant to be run daily after market close. Updates price data.
* tradingBot.py: the actual trading bot script.

Each script also has its own requirements.txt. These are important for the cloud deployment.

## Algorithm

Currently using a 125-day momentum algorithm blatantly stolen from Clenow's Trading Evolved. This may change in the future depending on results.

## Installation

* [ ] TODO: Finish implementing this section

### Alpaca Account Setup

1. Visit <https://alpaca.markets> and create an account. I recommend creating both a Paper account and a Live account. Use the Paper account for thorough testing before attempting live trades.
2. Under `Your API Keys`, click to generate a new API key. Make note of the secret, as it will only be displayed once.

### TD Ameritrade Developer Account Setup

1. Visit <https://developer.tdameritrade.com/apis> and register for an account.
2. Click on `My Apps` and then create a new app. Callback URL should be `http://localhost`
3. Note the Consumer Key for your app.

### Google Cloud Platform Initial Setup

1. Create an account on Google Cloud Platform and set up a new project.
2. I recommend pinning the following to the Navigation Menu: Cloud Functions, Cloud Scheduler, BigQuery and Storage.
3. In BigQuery, create a dataset called `equity_data`. Also make a note of your database name
4. In Storage, upload a text file containing your Alpaca credentials in the following format `<KEY ID>,<SECRET>`
5. In Storage, upload a text file containg your TD Ameritrade API key

## Usage

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
