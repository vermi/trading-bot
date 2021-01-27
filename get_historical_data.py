import pandas as pd
import numpy as np
import requests
from yahoofinancials import YahooFinancials as yf
from bs4 import BeautifulSoup
import string
import time
from progress.bar import Bar

# Get a current list of all the stock symbols for the NYSE
alpha = list(string.ascii_uppercase)

symbols = []
start_date = "2020-07-25"
end_date = "2021-01-25"
freq = "daily"

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

data_list = []

bar = Bar("Fetching price history", max=len(symbols))
for sym in symbols_clean:
    data = yf(sym)

    hist = data.get_historical_price_data(start_date, end_date, freq)
    data_list.append(hist)
    bar.next()
    time.sleep(0.5)
bar.finish()

# Create a list for each data point and loop through the json, adding the data to the lists
symbl_l, open_l, high_l, low_l, close_l, volume_l, date_l = [], [], [], [], [], [], []

bar = Bar("Parsing data", max=len(data_list))
for data in data_list:
    try:
        symbl_name = list(data.keys())[0]
    except KeyError:
        symbl_name = np.nan
    try:
        for each in data[symbl_name]["prices"]:
            symbl_l.append(symbl_name)
            open_l.append(each["open"])
            high_l.append(each["high"])
            low_l.append(each["low"])
            close_l.append(each["close"])
            date_l.append(each["formatted_date"])
    except KeyError:
        pass
    bar.next()
bar.finish()

# Create a df from the lists
print("Building DataFrame...")
df = pd.DataFrame(
    {
        "symbol": symbl_l,
        "openPrice": open_l,
        "highPrice": high_l,
        "lowPrice": low_l,
        "closePrice": close_l,
        "date": date_l,
    }
)

# Save to csv
df.to_csv(r"../data/back_data.csv")
print("CSV file saved successfully.")
