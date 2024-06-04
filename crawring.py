import pandas as pd
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup as bs
from io import StringIO
from stock_data import Code

my_code = Code(host="localhost", user="root", password="1234", database="test")
my_code.connect_to_database()
symbol_list = my_code.fetch_company_codes()

def get_stock_data(code, page):
    url = f'https://finance.naver.com/item/sise_day.nhn?code={code}&page={page}'
    headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36'}
    response = requests.get(url, headers=headers)
    html = bs(response.text, 'html.parser')
    html_table = html.select("table")
    table = pd.read_html(StringIO(str(html_table)))
    data = pd.DataFrame(table[0].dropna())  # dropna() 메서드를 호출하여 NaN 값을 제거합니다.
    return data

for stock_code in symbol_list:
    for page in range(1, 21):
        stock_data = get_stock_data(stock_code, page)
        print(stock_data)
        my_code.connect_to_database()
        my_code.insert_stock_price(stock_data, stock_code)
