# 테스트용 bought_prices 딕셔너리와 stock_dict 생성
bought_prices = {'AAPL': 150.0, 'GOOG': 2500.0, 'MSFT': 300.0}
stock_dict = {'AAPL': 10, 'GOOG': 5, 'MSFT': 8}

# 예시로 사용할 함수 정의
def sell(sym, qty):
    print(f"Selling {qty} shares of {sym}")

def send_message(msg):
    print(msg)

# 수정된 부분 테스트
for sym, qty in stock_dict.items():
    bit = 200
    bought_price = float(bought_prices[sym])    
    sell_profit_price = bought_price*10
    print("bought_price "+str(sell_profit_price))
    if bought_price > sell_profit_price:
        send_message(f"{sym} 종목의 가격이 2% 상승하여 익절합니다.")
    if bought_price < sell_profit_price:
        send_message(f"{sym}의  손절합니다")
