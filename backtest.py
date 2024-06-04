import datetime
from collections import defaultdict
from stock_back_test import Code

my_code = Code(host="localhost", user="root", password="1234", database="stock")

def backtest(strategy_func, start_date, end_date, initial_cash):
    print("테스트를 시작합니다")
    # 초기 자본금 설정
    cash = initial_cash
    stock_balance = defaultdict(int)
    bought_info = {}
    total_profit = 0
    buy_signal_count = defaultdict(int)  # 'buy' 시그널 발생 횟수를 기록할 딕셔너리
    stock_profit = defaultdict(int)  # 매수한 종목별 수익률을 기록할 딕셔너리

    # 데이터베이스 연결
    my_code.connect_to_database()

    # 백테스팅 기간 설정
    current_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    while current_date <= end_date:
        

        symbol_list = my_code.fetch_company_codes(current_date)[:100]  # 현재 날짜를 전달하여 종목 리스트를 가져옴
        # 전략 함수를 이용하여 매수/매도 시점 결정
        stock_data = my_code.fetch_stock_data_from_database(symbol_list)
        # 해당 날짜에 매수할 수 있는 종목 수
        max_buyable_symbols = min(10, len(symbol_list) - len(buy_signal_count))
        buy_count = 0  # 해당 날짜에 매수한 종목 수
        for symbol in symbol_list:
            if buy_count >= max_buyable_symbols:
                break  # 종목 수 제한에 도달하면 종료
            signal = strategy_func(stock_data, symbol, current_date)
           
            if signal == 'buy':
                current_price = get_opening_price(symbol, current_date, stock_data)  # 시가로 수정
                if current_price is not None:  # 가격 정보가 유효한 경우에만 처리
                    # 매수 가능한 금액 계산
                    buyable_cash = cash * 0.1  # 지분 비율은 25%
                    buy_qty = int(buyable_cash // current_price)
                    if buy_qty > 0:
                        buy_signal_count[symbol] += 1  
                        buy_count += 1
                        # 매수
                        cash -= buy_qty * current_price
                        stock_balance[symbol] += buy_qty
                        # 매수한 종목의 정보 저장
                        bought_info[symbol] = {'price': current_price, 'date': current_date}

        # 매도 로직 추가
        sold_symbols = []  # 매도한 종목을 따로 저장
        for symbol, info in bought_info.items():
            if current_date > end_date:
                last_price = get_closing_price(symbol, end_date, stock_data)
            else:
                last_price = get_closing_price(symbol, current_date, stock_data)
            stock_profit[symbol] += (last_price - info['price']) * stock_balance[symbol]
            commission = 0.985  # 매도 수수료 (일반적으로 주식거래 수수료는 약 0.015 ~ 0.25%)
            cash += stock_balance[symbol] * last_price * commission
            total_profit += stock_profit[symbol]
            stock_balance[symbol] = 0
            sold_symbols.append(symbol)  # 매도한 종목 저장
        # 매도한 종목은 딕셔너리에서 삭제
        for symbol in sold_symbols:
            del bought_info[symbol]

        # 다음 날짜로 이동   
        profit = ((cash - initial_cash) / initial_cash)*100
        print("현재날짜 : "+str(current_date)+"  현재 금액 : "+str(cash)+"  수익률 :"+str(profit))
        current_date += datetime.timedelta(days=1)

    # 데이터베이스 연결 종료
    my_code.close_database_connection()

    # 최종 수익률 계산
    return (cash - initial_cash) / initial_cash * 100, stock_profit,cash

def simple_ma_strategy(stock_data, symbol, current_date):
    # 현재 날짜의 5일 이동평균을 가져옵니다.
    moving_avg_5days = my_code.fetch_stock_price_5day(symbol, current_date)
    moving_avg_20days = my_code.fetch_stock_price_20day(symbol, current_date)
    # 현재 날짜의 주식 데이터 추출
    stock_data_today = stock_data[stock_data['stock_date'] == current_date]    
    current_price = get_opening_price(symbol, current_date, stock_data)
    # 주식 데이터가 없는 경우 'hold' 신호 반환
    if stock_data_today.empty or current_price is None or moving_avg_5days is None or moving_avg_20days is None:
        return 'hold'   
    
    # 해당 종목의 주식 데이터 추출
    stock_data_symbol = stock_data_today[stock_data_today['stock_code_no'] == symbol] 
    # 해당 종목의 주식 데이터가 없는 경우 'hold' 신호 반환
    if stock_data_symbol.empty:
        return 'hold'
    # 최근 날짜의 이동평균값 가져오기
    recent_moving_avg_5days = moving_avg_5days
    recent_moving_avg_20days = moving_avg_20days

    # 이동평균 크로스오버 매매 신호 결정
    if recent_moving_avg_5days is not None and recent_moving_avg_20days is not None and current_price is not None:
        if recent_moving_avg_5days > recent_moving_avg_20days and recent_moving_avg_5days > current_price:
            return 'buy'
        else:
            return 'hold'
    else:
        return 'hold'


def get_opening_price(symbol, date, stock_data):
    try:      
        price = my_code.fetch_stock_data_symbol(symbol, date)
        if price is not None:
            return price
        else:
            print(f"해당 날짜({date})에 종목 코드({symbol})에 대한 데이터가 없습니다.")
            # 다음 날짜의 데이터 가져오기
            next_date = date + datetime.timedelta(days=1)
            return get_opening_price(symbol, next_date, stock_data)
    except Exception as e:
        print(f"Error in getting opening price: {e}")
        return None

def get_closing_price(symbol, date, stock_data):
    try:
        price = my_code.fetch_stock_closing_symbol(symbol, date)
        if price is not None:
            return price
        else:
            print(f"해당 날짜({date})에 종목 코드({symbol})에 대한 데이터가 없습니다.")
            # 다음 날짜의 데이터 가져오기
            next_date = date + datetime.timedelta(days=1)
            return get_closing_price(symbol, next_date, stock_data)
    except Exception as e:
        print(f"Error in getting closing price: {e}")
        return None

 

# 백테스팅 기간 설정
start_date = '2024-05-01'
end_date = '2024-05-08'

# 초기 자본금 설정
initial_cash = 500000  # 100만원
666
# 백테스팅 실행
result, stock_profit,final_value = backtest(simple_ma_strategy, start_date, end_date, initial_cash)

# 종목별 수익률 출력
for symbol, profit in stock_profit.items():
    print(f"{symbol}: {profit:.2f} 원")

print(f"총 수익률: {result:.2f}%")
print(f"총 현재금액: {final_value:.2f}원")
