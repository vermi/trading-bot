# Python Trading Bot (and accessories)

This is a small suite of tools for use in cloud-based day trading activities.

## Contents

* nyseBackData.py: a script to download historical data for NYSE ticker symbols.
* getData.py: a script that is meant to be run daily after market close. Updates price data.
* tradingBot.py: the actual trading bot script.

Each script also has its own requirements.txt. These are important for the installation.

## Installation

* [ ] TODO: Implement this section

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
