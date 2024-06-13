import pymysql
import pandas as pd
class Code:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.connect_to_database()  # 생성자에서 데이터베이스에 연결

    def connect_to_database(self):
        try:
            self.conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
        except pymysql.MySQLError as e:
            print(f"Error connecting to database: {e}")
            self.conn = None

    def fetch_company_codes(self):
        if self.conn is None:
            print("No database connection.")
            return []
        
        try:
            # 커서 생성
            with self.conn.cursor() as c:
                # 쿼리 실행
                c.execute("SELECT stock_code_no FROM stock.stock_price WHERE stock_date = (SELECT MAX(stock_date) FROM stock.stock_price) ORDER BY stock_volume DESC LIMIT 100 OFFSET 40 ")

                result = c.fetchall()
                symbol_list = []
                
                # 결과를 리스트에 담기
                for row in result:
                    symbol_list.append(row[0])
            return symbol_list
        except pymysql.MySQLError as e:
            print(f"Database error: {e}")
            return []
        

        
    def company_code(self):
        if self.conn is None:
            print("No database connection.")
            return []
        
        try:
            # 커서 생성
            with self.conn.cursor() as c:
                # 쿼리 실행
                c.execute("SELECT stock_code_no FROM stock_code")

                result = c.fetchall()
                symbol_list = []
                
                # 결과를 리스트에 담기
                for row in result:
                    symbol_list.append(row[0])
            return symbol_list
        except pymysql.MySQLError as e:
            print(f"Database error: {e}")
            return []
        


        





            #  데이터 입력관련
    def get_latest_stock_data(self, code):
        if self.conn is None:
            print("데이터베이스에 연결되지 않았습니다.")
            return None

        try:
            with self.conn.cursor() as c:
                query = f"SELECT * FROM stock_price WHERE stock_code_no = '{code}' ORDER BY stock_date ASC LIMIT 1"
                c.execute(query)
                latest_data = c.fetchone()
                return latest_data
        except pymysql.MySQLError as e:
            print(f"데이터베이스 오류: {e}")
            return None
        

            #  데이터 입력관련
    def compare_and_insert_data(self, code, stock_data):
        latest_data = self.get_latest_stock_data(code)

        if latest_data is None:
            # 데이터베이스에 기존 데이터가 없을 경우
            self.insert_stock_price(stock_data, code)
            return

        # 데이터베이스에서 최신 데이터 추출
        latest_date_db = latest_data[1]  # 날짜가 두 번째 열이라 가정합니다.

        # 웹에서 가져온 최신 데이터 추출
        latest_date_web = stock_data['날짜'].iloc[0]  # '날짜'가 날짜 열이라고 가정합니다.

        if latest_date_web != latest_date_db:
            self.insert_stock_price(stock_data, code)
        else:
            print("데이터가 이미 최신입니다. 삽입이 필요하지 않습니다.")


    # 데이터 입력관련
    def change_date_format(self, date_str):
        # 날짜 형식 변경 함수
        return date_str.replace('.', '-')


    # 데이터 입력관련 백업용
    def insert_stock_price_back(self, data, stock_code):
        if self.conn is None:
            print("No database connection.")
            return
        
        try:
            with self.conn.cursor() as c:
                for index, row in data.iterrows():
                    # 각 행에서 날짜, 종가, 시가, 고가, 저가, 거래량 등을 추출합니다.
                    date = self.change_date_format(row['날짜'])
                    close_price = row['종가']
                    start_price = row['시가']
                    high_price = row['고가']
                    low_price = row['저가']
                    volume = row['거래량']
                    
                    query = """
                        INSERT INTO stock.stock_price (
                            stock_code_no,
                            stock_date,
                            stock_close_price,
                            stock_start_price,
                            stock_high_price,
                            stock_low_price,
                            stock_volume
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """

                    # 쿼리 실행
                    c.execute(query, (str(stock_code), str(date), str(close_price), str(start_price), str(high_price), str(low_price), str(volume)))

            # 모든 데이터 삽입 후에 한 번만 커밋합니다.
            self.conn.commit()
            print("Data inserted successfully.")
               
        except pymysql.MySQLError as e:
            print(f"Error inserting data into database: {e}")

        # 데이터 입력관련
    def change_date_format_today(self, date_str):
            # 날짜 형식 변경 함수
            dates = date_str.split('\n') if isinstance(date_str, str) else [str(date_str)]  # 문자열인 경우에만 split 메서드를 적용
            return dates[0].replace('.', '-')

        # 데이터 입력관련 매일
    def insert_stock_price_today(self, data, stock_code):
            if self.conn is None:
                print("No database connection.")
                return
            
            try:
                with self.conn.cursor() as c:
                    
                    # 각 행에서 날짜, 종가, 시가, 고가, 저가, 거래량 등을 추출합니다.
                    date = self.change_date_format_today(data['날짜'].iloc[0])  # 'iloc[0]'를 사용하여 첫 번째 요소를 선택
                    close_price = data['종가'].iloc[0]  # 'iloc[0]'를 사용하여 첫 번째 요소를 선택
                    start_price = data['시가'].iloc[0]  # 'iloc[0]'를 사용하여 첫 번째 요소를 선택
                    high_price = data['고가'].iloc[0]  # 'iloc[0]'를 사용하여 첫 번째 요소를 선택
                    low_price = data['저가'].iloc[0]  # 'iloc[0]'를 사용하여 첫 번째 요소를 선택
                    volume = data['거래량'].iloc[0]  # 'iloc[0]'를 사용하여 첫 번째 요소를 선택
                    
                    query = """
                        INSERT INTO stock.stock_price (
                            stock_code_no,
                            stock_date,
                            stock_close_price,
                            stock_start_price,
                            stock_high_price,
                            stock_low_price,
                            stock_volume
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """

                    # 쿼리 실행
                    c.execute(query, (str(stock_code), str(date), str(close_price), str(start_price), str(high_price), str(low_price), str(volume)))

                # 모든 데이터 삽입 후에 한 번만 커밋합니다.
                self.conn.commit()
                print("Data inserted successfully.")
                
            except pymysql.MySQLError as e:
                print(f"Error inserting data into database: {e}")



#  5일이동평균선
    def fetch_stock_price_5day(self, stock_code):
            if self.conn is None:
                print("No database connection.")
                return None
            
            try:
                with self.conn.cursor() as c:
                    c.execute("SELECT * FROM stock_price WHERE stock_code_no = %s", (stock_code))
                    result = c.fetchall()
                        
                    if not result:
                        print("No data found for the given stock code.")
                        return None

                    df = pd.DataFrame(result, columns=['id', 'stock_code_no', 'stock_date', 'stock_close_price', 'stock_start_price', 'stock_high_price', 'stock_low_price', 'stock_volume'])
                    df['stock_date'] = pd.to_datetime(df['stock_date'])

                    # 데이터프레임을 역순으로 가져와서 주식 종가 열을 추출합니다.
                    reversed_df = df[::-1]
                    stock_close_price_reverse = reversed_df['stock_close_price']

                    # 역순으로 정렬된 주식 종가 열을 사용하여 이동평균을 계산합니다.
                    moving_avg_5days_reverse = stock_close_price_reverse.rolling(window=5).mean() 
                    recent_moving_avg = moving_avg_5days_reverse.iloc[-1]                   
                    return recent_moving_avg
            except pymysql.MySQLError as e:
                print(f"Database error: {e}")
                return None



                #  20일이동평균선
    def fetch_stock_price_20day(self, stock_code):
            if self.conn is None:
                print("No database connection.")
                return None
            
            try:
                with self.conn.cursor() as c:
                    c.execute("SELECT * FROM stock_price WHERE stock_code_no = %s", (stock_code,))
                    result = c.fetchall()
                        
                    if not result:
                        print("No data found for the given stock code.")
                        return None

                    df = pd.DataFrame(result, columns=['id','stock_code_no','stock_date', 'stock_close_price', 'stock_start_price', 'stock_high_price', 'stock_low_price', 'stock_volume'])
                    df['stock_date'] = pd.to_datetime(df['stock_date'])

                    # 데이터프레임을 역순으로 가져와서 주식 종가 열을 추출합니다.
                    reversed_df = df[::-1]
                    stock_close_price_reverse = reversed_df['stock_close_price']

                    # 역순으로 정렬된 주식 종가 열을 사용하여 이동평균을 계산합니다.
                    moving_avg_20days_reverse = stock_close_price_reverse.rolling(window=20).mean()  
                    recent_moving_avg = moving_avg_20days_reverse.iloc[-1]                   
                    return recent_moving_avg
            except pymysql.MySQLError as e:
                print(f"Database error: {e}")
                return None


  #  100일이동평균선
    def fetch_stock_price_100day(self, stock_code):
            if self.conn is None:
                print("No database connection.")
                return None
            
            try:
                with self.conn.cursor() as c:
                    c.execute("SELECT * FROM stock_price WHERE stock_code_no = %s", (stock_code,))
                    result = c.fetchall()
                        
                    if not result:
                        print("No data found for the given stock code.")
                        return None

                    df = pd.DataFrame(result, columns=['id','stock_code_no','stock_date', 'stock_close_price', 'stock_start_price', 'stock_high_price', 'stock_low_price', 'stock_volume'])
                    df['stock_date'] = pd.to_datetime(df['stock_date'])

                    # 데이터프레임을 역순으로 가져와서 주식 종가 열을 추출합니다.
                    reversed_df = df[::-1]
                    stock_close_price_reverse = reversed_df['stock_close_price']

                    # 역순으로 정렬된 주식 종가 열을 사용하여 이동평균을 계산합니다.
                    moving_avg_100days_reverse = stock_close_price_reverse.rolling(window=100).mean()
                    recent_moving_avg = moving_avg_100days_reverse.iloc[-1]                     
                    return recent_moving_avg
            except pymysql.MySQLError as e:
                print(f"Database error: {e}")
                return None
            
    
    def close_database_connection(self):
        # 데이터베이스 연결 종료
        if self.conn:
            self.conn.close()
            print("데이터베이스 연결이 닫혔습니다.")
