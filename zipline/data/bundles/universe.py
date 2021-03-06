from enum import Enum
import pandas as pd
import alpaca_trade_api

SP500_WIKI_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
SP100_WIKI_URL = 'https://en.wikipedia.org/wiki/S%26P_100'
NASDAQ100_WIKI_URL = 'https://en.wikipedia.org/wiki/NASDAQ-100'


class Universe(Enum):
    ALL = "ALL"
    SP100 = "S&P 100"
    SP500 = "S&P 500"
    NASDAQ100 = "NASDAQ 100"


def all_alpaca_assets(client: alpaca_trade_api.REST) -> list:
    return [_.symbol for _ in client.list_assets()]

def read_all_alpaca_assets(filename) -> list:
    with open(filename) as f:
        content = f.readlines()
    # you may also want to remove whitespace characters like `\n` at the end of each line
    return [x.strip() for x in content]

def get_sp500() -> list:
    table = pd.read_html(SP500_WIKI_URL)
    df = table[0]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    return df.Symbol.tolist()


def get_sp100() -> list:
    table = pd.read_html(SP100_WIKI_URL)
    df = table[2]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    return df.Symbol.tolist()


def get_nasdaq100() -> list:
    table = pd.read_html(NASDAQ100_WIKI_URL)
    df = table[3]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    return df.Ticker.tolist()


if __name__ == '__main__':
    print(get_sp100()[:10])
    # print(get_sp500()[:10])