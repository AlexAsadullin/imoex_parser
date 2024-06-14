import pandas as pd
import requests
import json
from datetime import datetime
from datetime import date, timedelta

def save_data(name: str, data, extension: str):
    with open(f'{name}.{extension}', 'w') as file:
        if extension == 'json':
            file.write(json.dumps(data))
        elif extension == 'csv' and 'DataFrame' in str(type(data)):
            file.write(data.to_scv())
        else:
            print('unable to save your data type yet, please, try to save it by yourself')


def get_general_info(file_type='json', encoding='utf-8'):
    url = f"https://iss.moex.com/iss/statistics/engines/stock/quotedsecurities.{file_type}"
    print(url)
    request = requests.get(url)
    request.encoding = encoding
    data = request.json()
    save_data('imoex_general', data, extension='json')
    all_stocks_df = pd.DataFrame(data['quotedsecurities']['data'], columns=data['quotedsecurities']['columns'])
    return all_stocks_df


def get_stock_history(begin_date, end_date, ticker: str, encoding='utf-8'):
    engines = {'stock': 'stock', }
    markets = {'shares': 'shares', 'bonds': 'bonds', 'foreignshares': 'foreignshares'}
    url = f"https://iss.moex.com/iss/history/engines/{engines['stock']}/markets/{markets['shares']}/securities/{ticker}.json?from={begin_date}&till={end_date}&marketprice_board=1"
    #url = 'https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/MOEX.json?from=2023-01-01&till=2023-05-31&marketprice_board=1'
    print(url)
    request = requests.get(url)
    save_data(f'{ticker}_{begin_date}_{end_date}', request.text, extension='json')
    data = json.loads(request.text)
    df = pd.DataFrame(data['history']['data'], columns=data['history']['columns'])
    return df


def get_more_4months(begin_date: datetime, end_date: datetime, ticker):
    sec_to_mon = 0
    difference = end_date.timestamp() - begin_date.timestamp()
    sec_to_day = 1 / (60 * 60 * 24 * 30)
    for i in range(int(difference * sec_to_day)):



    imoex_history = get_stock_history(begin_date=begin, end_date=actual_time, ticker='MOEX')


def dates_range(year_begin, month_begin, day_begin, year_end, month_end, day_end, endnow: bool):
    begin = date(year_begin, month_begin, day_begin)
    ending = date.today() - timedelta(days=1)
    delta = timedelta(days=1)
    dates = []
    while begin <= ending:
        dates.append(begin.strftime('%Y-%m-%d'))
        begin += delta
    return dates



if __name__ == '__main__':
    actual_time = datetime.now()
    print(actual_time)
    begin = datetime.strptime('2020-05-30', '%Y-%m-%d')
    history = get_stock_history(begin_date=begin, end_date=actual_time, ticker='MOEX')



    print(len(imoex_history))
    imoex_history.to_csv(f'imoex_{begin}.csv')
