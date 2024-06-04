import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from io import StringIO
from stock_data import Code
# 주식 데이터를 가져오는 함수
def get_stock_data(code, page):
    url = f'https://finance.naver.com/item/sise_day.nhn?code={code}&page={page}'
    headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36'}
    response = requests.get(url, headers=headers)
    html = bs(response.text, 'html.parser')
    html_table = html.select("table")
    table = pd.read_html(StringIO(str(html_table)))
    if not table:
        return None
    data = pd.DataFrame(table[0].dropna())  # dropna() 메서드를 호출하여 NaN 값을 제거합니다.
    return data

# 주식 코드를 가져와 데이터베이스에 삽입하는 메인 함수
def main():
    # 데이터베이스 연결
    my_code = Code(host="localhost", user="root", password="1234", database="stock")
    my_code.connect_to_database()
    symbol_list = my_code.fetch_company_codes()

    # 각 주식 코드에 대해 최신 데이터를 가져와 데이터베이스에 삽입
    for stock_code in symbol_list:
        page = 1
        stock_data = get_stock_data(stock_code, page)
        if stock_data is not None and not stock_data.empty:
            latest_data = stock_data.tail(10)  # 가장 최근 날짜의 데이터만 선택 항목이 10개중 가장 위에있는데이터
            my_code.connect_to_database()
            my_code.insert_stock_price_today(latest_data,stock_code)
            stock_data = get_stock_data(stock_code, page)

if __name__ == "__main__":
    main()
