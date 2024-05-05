import pandas as pd
import requests
import json
from datetime import datetime
from urllib import parse


def save_data(name: str, data, extension='json'):
    with open(f'{name}.{extension}', 'w') as file:
        if extension == 'json':
            file.write(json.dumps(data))
        elif extension == 'csv' and 'DataFrame' in str(type(data)):
            file.write(data.to_scv())


def get_general_info(file_type='json', encoding='utf-8'):
    url = f"https://iss.moex.com/iss/statistics/engines/stock/quotedsecurities.{file_type}"
    print(url)
    request = requests.get(url)
    request.encoding = encoding
    data = request.json()
    save_data('imoex_test', data)
    all_stocks_df = pd.DataFrame(data['quotedsecurities']['data'], columns=data['quotedsecurities']['columns'])
    return all_stocks_df


def get_stock_history(begin_date, end_date, encoding='utf-8'):
    engines = {'stock': 'stock', }
    markets = {'shares': 'shares', 'bonds': 'bonds', 'foreignshares': 'foreignshares'}
    # url = f"https://iss.moex.com/iss/history/engines/{engines['stock']}/markets/{markets['shares']}/securities/MOEX.json?from={begin_date}&till={end_date}&marketprice_board=1"
    url = 'https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/MOEX.json?from=2023-01-01&till=2023-01-31&marketprice_board=1'
    print(url)
    request = requests.get(url)
    save_data('test_1certain', request.text)
    data = json.loads(request.text)
    print(data['history']['columns'])
    print(len(data['history']['data']))
    df = pd.DataFrame(data['history']['data'], columns=data['history']['columns'])
    with open(f'IMOEXstock.csv', 'w') as f:
        f.write(df.to_csv())
        print('sucsess', end=' ')


if __name__ == '__main__':
    # parse from "https://iss.moex.com/iss/", choose catalog
    actual_time = datetime.now().strftime('%Y-%m-%d')
    print(actual_time)
    begin = '2021-05-30'
    get_stock_history(begin_date=begin, end_date=actual_time)
