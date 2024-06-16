import pandas as pd
import requests
import json
from datetime import datetime
from datetime import date, timedelta

DATE_FORMAT = '%Y-%m-%d'


def save_data(name: str, data, extension: str):
    with open(f'data/{name}.{extension}', 'w') as file:
        if extension == 'json':
            file.write(json.dumps(data))
        elif extension == 'csv' and 'DataFrame' in str(type(data)):
            file.write(data.to_csv())
        else:
            print('unable to save your data type yet, please, try to save it by yourself')


def dates_range(year_begin, month_begin, day_begin, year_end=None, month_end=None, day_end=None, endnow=True,
                noweekends=True):
    begin = date(year_begin, month_begin, day_begin)

    if endnow:
        ending = date.today() - timedelta(days=1)
    else:
        ending = date(year_end, month_end, day_end) - timedelta(days=1)

    delta = timedelta(days=1)
    dates = []

    if not noweekends:
        while begin <= ending:
            dates.append(begin.strftime(DATE_FORMAT))
            begin += delta
    else:
        while begin <= ending:
            weekday = begin.weekday()
            if weekday != 5 and weekday != 6:
                dates.append(begin.strftime(DATE_FORMAT))
            begin += delta

    return dates


def get_general_info(file_type='json', encoding='utf-8'):
    url = f"https://iss.moex.com/iss/statistics/engines/stock/quotedsecurities.{file_type}"
    print(url)
    request = requests.get(url)
    request.encoding = encoding
    data = request.json()
    save_data('imoex_general', data, extension='json')
    all_stocks_df = pd.DataFrame(data['quotedsecurities']['data'], columns=data['quotedsecurities']['columns'])
    return all_stocks_df


def get_stock_prices(begin_date, end_date, ticker: str, encoding='utf-8'):
    engines = {'stock': 'stock', }
    markets = {'shares': 'shares', 'bonds': 'bonds', 'foreignshares': 'foreignshares'}
    url = f"https://iss.moex.com/iss/history/engines/{engines['stock']}/markets/{markets['shares']}/securities/{ticker}.json?from={begin_date}&till={end_date}&marketprice_board=1"
    # url = 'https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/MOEX.json?from=2023-01-01&till=2023-05-31&marketprice_board=1'
    print(url)
    request = requests.get(url)
    data = json.loads(request.text)
    df = pd.DataFrame(data['history']['data'], columns=data['history']['columns'])
    return df


def get_stock_history(MAX_LOAD_ONCE=100):
    dates = dates_range(2020, 5, 30, endnow=True, noweekends=True)
    ticker = 'MOEX'
    stock_df = pd.DataFrame()
    for i in range(0, len(dates) // MAX_LOAD_ONCE + 1):
        if i * MAX_LOAD_ONCE + MAX_LOAD_ONCE < len(dates):
            df = get_stock_prices(ticker=ticker,
                                  begin_date=dates[i * MAX_LOAD_ONCE],
                                  end_date=dates[i * MAX_LOAD_ONCE + MAX_LOAD_ONCE])
            stock_df = pd.concat([stock_df, df], ignore_index=True)
        else:
            df = get_stock_prices(ticker=ticker,
                                  begin_date=dates[i * MAX_LOAD_ONCE],
                                  end_date=dates[len(dates) - 1])
            stock_df = pd.concat([stock_df, df], ignore_index=True)

    save_data(f'{ticker}_{dates[0]}_{dates[-1]}', stock_df, 'csv')


if __name__ == '__main__':
    get_stock_history()
