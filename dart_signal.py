import requests
import pandas as pd
from bs4 import BeautifulSoup
from tabulate import tabulate
from datetime import datetime, timedelta
import time
import telegram
import asyncio
from sqlalchemy import create_engine, text
import re
import kis_auth as ka
import kis_domstk as kb
import sys
import math
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

ka.auth()

# MySQL 연결 정보 설정 (연결 풀링 및 연결 유지 설정)
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME_DART = os.getenv('DB_NAME_DART')
DB_NAME_PRICE = os.getenv('DB_NAME_PRICE')

db_url_dart = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME_DART}'
db_url_price = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME_PRICE}'

# 연결 풀링 및 연결 유지 설정
DB_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', 10))
DB_MAX_OVERFLOW = int(os.getenv('DB_MAX_OVERFLOW', 20))
DB_POOL_RECYCLE = int(os.getenv('DB_POOL_RECYCLE', 3600))
DB_POOL_TIMEOUT = int(os.getenv('DB_POOL_TIMEOUT', 30))

engine_dart = create_engine(
    db_url_dart,
    pool_size=DB_POOL_SIZE,          # 연결 풀 크기
    max_overflow=DB_MAX_OVERFLOW,    # 추가 연결 허용 수
    pool_recycle=DB_POOL_RECYCLE,    # 1시간마다 연결 재활용
    pool_pre_ping=True,              # 연결 사용 전 ping 테스트
    pool_timeout=DB_POOL_TIMEOUT     # 연결 대기 시간
)

engine_price = create_engine(
    db_url_price,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_recycle=DB_POOL_RECYCLE,
    pool_pre_ping=True,
    pool_timeout=DB_POOL_TIMEOUT
)

# 데이터베이스 연결 상태 확인 및 재연결 함수
def check_db_connection():
    """데이터베이스 연결 상태를 확인하고 필요시 재연결"""
    try:
        # 각 엔진의 연결 상태 확인
        with engine_dart.connect() as conn:
            conn.execute(text("SELECT 1"))
        with engine_price.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("데이터베이스 연결 상태: 정상")
        return True
    except Exception as e:
        print(f"데이터베이스 연결 오류: {e}")
        print("데이터베이스 재연결 시도 중...")
        try:
            # 연결 풀 재시작
            engine_dart.dispose()
            engine_price.dispose()
            time.sleep(5)  # 5초 대기 후 재연결
            return True
        except Exception as reconnect_error:
            print(f"데이터베이스 재연결 실패: {reconnect_error}")
            return False

# 데이터베이스 작업을 위한 안전한 래퍼 함수들
def safe_read_sql(query, engine, max_retries=3):
    """안전한 SQL 읽기 함수 (재시도 로직 포함)"""
    for attempt in range(max_retries):
        try:
            return pd.read_sql(query, con=engine)
        except Exception as e:
            print(f"SQL 읽기 오류 (시도 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 지수 백오프
                check_db_connection()  # 연결 상태 확인 및 재연결
            else:
                raise e

def safe_to_sql(df, table_name, engine, if_exists='append', max_retries=3):
    """안전한 SQL 쓰기 함수 (재시도 로직 포함)"""
    for attempt in range(max_retries):
        try:
            df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)
            print(f"데이터베이스 저장 성공: {table_name} 테이블에 {len(df)}개 행 저장")
            return True
        except Exception as e:
            print(f"SQL 쓰기 오류 (시도 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 지수 백오프
                check_db_connection()  # 연결 상태 확인 및 재연결
            else:
                print(f"데이터베이스 저장 최종 실패: {table_name}")
                raise e

# 텔레그램 메시지 전송
async def send_message(a):
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    bot = telegram.Bot(token)
    await bot.send_message(chat_id=chat_id, text=a)

# DART API 정보
api_key = os.getenv('DART_API_KEY')
url_1 = 'https://opendart.fss.or.kr/api/list.json'

import re
import requests
from bs4 import BeautifulSoup

def crawling_data(recept_no, report_nm):
    try:
        # 1) dcmNo 추출을 위한 HTML 파싱
        url_1 = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={recept_no}"
        headers = {
            "User-Agent": "Mozilla/5.0"
        }
        response = requests.get(url_1, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        script_tags = soup.find_all('script', type='text/javascript')
        script_content = ""
        for script in script_tags:
            if script.string:
                script_content += script.string

        match = re.search(r"node1\['dcmNo'\]\s*=\s*\"(\d+)\"", script_content)
        dcmNo = match.group(1)

        # 2) 실제 공시 본문 파싱
        url_2 = f"https://dart.fss.or.kr/report/viewer.do?rcpNo={recept_no}&dcmNo={dcmNo}&eleId=0&offset=0&length=0"
        response = requests.get(url_2, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        # 3) 모든 테이블 가져오기
        tables = soup.find_all('table')
        if not tables:
            raise ValueError("No tables found in the HTML content")

        # 4) 추출할 데이터 초기화
        amount = None
        amount_share = None
        amount_price = None
        start_dt = None
        end_dt = None
        purpose = None

        # 5) 보고서 유형별 데이터 파싱
        if '자기주식취득결정' in report_nm:
            # (1) 취득예정금액(원) 추출
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) > 2 and '2. 취득예정금액(원)' in cells[0].text.strip() and '보통주식' in cells[1].text.strip():
                        amount_text = cells[2].text.strip()
                        if amount_text:
                            if amount_text == '-':
                                amount = 0
                            else:
                                amount = int(amount_text.replace(",", ""))
                            break
                if amount is not None:
                    break

            # (2) 취득예상기간(시작일, 종료일) 추출
            for table in tables:
                rows = table.find_all('tr')
                for i, row in enumerate(rows):
                    cells = row.find_all('td')
                    
                    # (1) 첫 번째 TD에 '3. 취득예상기간'이 있는지 확인
                    if cells and '3. 취득예상기간' in cells[0].get_text(strip=True):
                        # (2) rowspan 값 가져오기(없으면 기본 2)
                        row_span = int(cells[0].get('rowspan', '2'))
                        
                        # (3) rowspan 범위 내 행들을 순회하며 시작일, 종료일 찾아 추출
                        for sub_i in range(i, i + row_span):
                            if sub_i >= len(rows):
                                break
                            sub_cells = rows[sub_i].find_all('td')
                            for j, sc in enumerate(sub_cells):
                                text = sc.get_text(strip=True)
                                if '시작일' in text and j + 1 < len(sub_cells):
                                    raw = sub_cells[j + 1].get_text(strip=True)
                                    start_dt = raw.replace('년 ', '-').replace('월 ', '-').replace('일', '')
                                elif '종료일' in text and j + 1 < len(sub_cells):
                                    raw = sub_cells[j + 1].get_text(strip=True)
                                    end_dt = raw.replace('년 ', '-').replace('월 ', '-').replace('일', '')
                        
                        # (4) 시작일, 종료일이 둘 다 추출됐다면 반복 종료
                        if start_dt and end_dt:
                            break
                if start_dt and end_dt:
                    break

            # (3) 취득목적 추출
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) > 1 and '5. 취득목적' in cells[0].text.strip():
                        purpose = cells[1].text.strip()
                        break
                if purpose:
                    break

        elif '자기주식취득신탁계약체결결정' in report_nm:
            # (1) 취득예정주식(주) 추출
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) > 2 and '9. 취득예정주식(주)' in cells[0].text.strip() and '보통주식' in cells[1].text.strip():
                        amount_share_text = cells[2].text.strip()
                        if amount_share_text:
                            if amount_share_text == '-':
                                amount_share = 0
                            else:
                                amount_share = int(amount_share_text.replace(",", ""))
                            break
                if amount_share is not None:
                    break

            # (2) 취득하고자 하는 주식의 가격(원) 추출
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) > 2:
                        cell_text = " ".join(cells[0].text.split()).replace("&nbsp;", "").strip()
                        if '10. 취득하고자 하는 주식의 가격(원)' in cell_text and '보통주식' in cells[1].text.strip():
                            amount_price_text = cells[2].text.strip()
                            if amount_price_text:
                                if amount_price_text == '-':
                                    amount_price = 0
                                else:
                                    amount_price = int(amount_price_text.replace(",", ""))
                                break
                if amount_price is not None:
                    break

            # (3) 계약기간(시작일, 종료일) 추출
            for table in tables:
                rows = table.find_all('tr')
                for i, row in enumerate(rows):
                    cells = row.find_all('td')
                    
                    # (1) 첫 번째 TD에 '2. 계약기간'이 있는지 확인
                    if cells and '2. 계약기간' in cells[0].get_text(strip=True):
                        # (2) rowspan 값 가져오기(없으면 기본 2)
                        row_span = int(cells[0].get('rowspan', '2'))
                        
                        # (3) rowspan 범위 내 행들을 순회하며 시작일, 종료일 찾아 추출
                        for sub_i in range(i, i + row_span):
                            if sub_i >= len(rows):
                                break
                            sub_cells = rows[sub_i].find_all('td')
                            for j, sc in enumerate(sub_cells):
                                text = sc.get_text(strip=True)
                                if '시작일' in text and j + 1 < len(sub_cells):
                                    raw = sub_cells[j + 1].get_text(strip=True)
                                    start_dt = raw.replace('년 ', '-').replace('월 ', '-').replace('일', '')
                                elif '종료일' in text and j + 1 < len(sub_cells):
                                    raw = sub_cells[j + 1].get_text(strip=True)
                                    end_dt = raw.replace('년 ', '-').replace('월 ', '-').replace('일', '')
                        
                        # (4) 시작일, 종료일이 둘 다 추출됐다면 반복 종료
                        if start_dt and end_dt:
                            break
                if start_dt and end_dt:
                    break

            # (4) 계약목적 추출
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) > 1 and '3. 계약목적' in cells[0].text.strip():
                        purpose = cells[1].text.strip()
                        break
                if purpose:
                    break

            # (5) 총금액(주식 수 × 주가)
            if amount_share is not None and amount_price is not None:
                amount = amount_share * amount_price

        else:
            # 해당 조건에 맞지 않는 경우
            print("error")

        return amount, start_dt, end_dt, purpose

    except Exception as e:
        print(f"Error fetching amount for recept_no {recept_no}: {e}")
        return None

def get_market_cap(stock_code):
    query = f"SELECT close, listed_stocks FROM stock_daily WHERE code = '{stock_code}' ORDER BY date DESC LIMIT 1;"
    df = safe_read_sql(query, engine_price)
    close = df.iloc[0, 0]
    listed_stocks = df.iloc[0, 1]
    market_cap = close * listed_stocks
    return int(market_cap)

def main():
    try:
        # 오늘 날짜와 어제 날짜를 함수 실행 시점에 갱신
        today = datetime.now().strftime("%Y%m%d")
        day_ago = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M")

        # DART에서 공시 정보 조회
        params = {
            'crtfc_key': api_key,
            'pblntf_ty': 'B',
            'sort': 'date',
            'sort_mth': 'desc',
            'bgn_de': day_ago,
            'end_de': today,
            'page_no': '1',
            'page_count': '100'
        }

        response = requests.get(url_1, params=params)
        data_1 = response.json()

        # list 키의 값을 DataFrame으로 변환
        df = pd.DataFrame(data_1['list'])

        # 코넥스, 기타법인 제외
        df = df[(df['corp_cls'] != 'E') & (df['corp_cls'] != 'N')]

        # 필요한 열만 선택
        df = df[['corp_code', 'corp_name', 'stock_code', 'rcept_dt', 'report_nm', 'rcept_no']]

        # '자기주식취득결정' or '자기추식취득신탁계약체결결정'을 포함하는 행만 필터링
        df_filtered = df[df['report_nm'].str.contains('자기주식취득결정|자기주식취득신탁계약체결결정')]

        # 특정 텍스트를 포함하는 행 제외
        exclude_texts = [
            '기재정정', '첨부정정', '첨부추가', '변경등록', 
            '연장결정', '발행조건확정', '정정명령부과', '정정제출요구', '자회사'
        ]
        pattern = '|'.join(exclude_texts)  # OR 조건을 위한 정규식 패턴 생성

        df_filtered = df_filtered[~df_filtered['report_nm'].str.contains(pattern)]

        # MySQL 데이터베이스에서 stock_buy_back 테이블 불러오기
        query = "SELECT * FROM stock_buy_back"
        df_old = safe_read_sql(query, engine_dart)

        # 새로운 데이터가 있는지 확인, recept_no로 비교
        new_entries = df_filtered[~df_filtered['rcept_no'].isin(df_old['rcept_no'])].copy()

        if not new_entries.empty:
            new_entries['stock_code'] = 'A' + new_entries['stock_code']
            new_entries[['amount', 'start_dt', 'end_dt', 'purpose']] = new_entries.apply(
                lambda row: pd.Series(crawling_data(row['rcept_no'], row['report_nm'])), axis=1)
            new_entries['market_cap'] = new_entries.apply(
                lambda row: get_market_cap(row['stock_code']), axis=1)
            # crawling 실패로 amount/market_cap이 None인 행 제거
            new_entries = new_entries.dropna(subset=['amount', 'market_cap'])
            new_entries = new_entries[new_entries['market_cap'] > 0]
            if new_entries.empty:
                print("유효한 데이터가 없습니다 (크롤링 실패)")
                return None
            new_entries['ratio'] = (new_entries['amount'] / new_entries['market_cap'] * 100).round(2)
            new_entries['amount'] = (new_entries['amount'] / 1e8).round(0).astype(int)
            new_entries['market_cap'] = (new_entries['market_cap'] / 1e8).round(0).astype(int)
            
            # 원하는 컬럼 순서
            column_order = [
                'corp_code', 'corp_name', 'stock_code', 'rcept_dt', 'report_nm', 'rcept_no',
                'amount', 'market_cap', 'ratio', 'start_dt', 'end_dt', 'purpose'
            ]

            # 컬럼 순서 재정렬
            new_entries = new_entries[column_order]
            
            # 데이터프레임 가독성 좋은 서식으로 문자변수로 바꾸기
            rows_length = len(new_entries)
            for i in range(0, rows_length):
                corp_code = new_entries.iloc[i, 0]
                corp_name = new_entries.iloc[i, 1]
                stock_code = new_entries.iloc[i, 2]
                rcept_dt = new_entries.iloc[i, 3]
                report_nm = new_entries.iloc[i, 4]
                recept_no = new_entries.iloc[i, 5]
                amount = new_entries.iloc[i, 6]
                amount = f"{amount:,}"
                market_cap = new_entries.iloc[i, 7]
                market_cap = f"{market_cap:,}"
                ratio = new_entries.iloc[i, 8]
                start_dt = new_entries.iloc[i, 9]
                end_dt = new_entries.iloc[i, 10]
                purpose = new_entries.iloc[i, 11]

                formatted_str = (f"report_nm: {report_nm}\nrcept_dt: {rcept_dt}\n"
                                 f"corp_name: {corp_name}\nstock_code: {stock_code}\n"
                                 f"amount: {amount} 억원\nmarket_cap: {market_cap} 억원\n"
                                 f"ratio: {ratio}%\nstart_dt: {start_dt}\nend_dt: {end_dt}\n"
                                 f"purpose: {purpose}"
                                 f"\n\nreport_link: https://dart.fss.or.kr/dsaf001/main.do?rcpNo={recept_no}")

                # 텔레그램으로 전송
                asyncio.run(send_message(formatted_str))

            print(new_entries)
            # 새로운 데이터를 데이터베이스에 삽입
            safe_to_sql(new_entries, 'stock_buy_back', engine_dart, if_exists='append')
            print("새로운 데이터가 추가되었습니다.")
            return new_entries

        else:
            print(f"새 자사주 공시 없음 : {current_time}")

    except Exception as e:
        print(f"Error 발생: {e}")

# 주식 시장가 주문
def buy(code, qty):
    rt_data = kb.get_order_cash(ord_dv="buy",itm_no=code, qty=qty, unpr="0")
    if rt_data is None:
        print(f"매수 주문 실패: 종목코드 {code} (API 응답 없음)")
        return None
    print(rt_data.KRX_FWDG_ORD_ORGNO + "+" + rt_data.ODNO + "+" + rt_data.ORD_TMD)
    return rt_data

def sell(code, qty):
    rt_data = kb.get_order_cash(ord_dv="sell",itm_no=code, qty=qty, unpr="0")
    if rt_data is None:
        print(f"매도 주문 실패: 종목코드 {code} (API 응답 없음)")
        return None
    print(rt_data.KRX_FWDG_ORD_ORGNO + "+" + rt_data.ODNO + "+" + rt_data.ORD_TMD)
    return rt_data
    
# [국내주식] 기본시세 > 주식현재가 시세 (종목번호 6자리)
def current_price(code):
    rt_data = kb.get_inquire_price(itm_no=code)
    current_price = int(rt_data['stck_prpr'].iloc[0])
    return current_price

def get_execution_detail(stock_code):
    """당일 특정 종목의 최신 체결 정보를 조회"""
    today = datetime.now().strftime("%Y%m%d")
    try:
        time.sleep(1)  # 주문 체결 반영 대기
        df = kb.get_inquire_daily_ccld_lst(dv="01", inqr_strt_dt=today, inqr_end_dt=today)
        if df is not None and not df.empty:
            code = stock_code[1:] if stock_code.startswith('A') else stock_code
            filtered = df[df['pdno'] == code]
            if not filtered.empty:
                return filtered.iloc[-1]  # 가장 최근 주문
        return None
    except Exception as e:
        print(f"체결 정보 조회 오류: {e}")
        return None

def format_execution_info(detail):
    """체결 상세 정보를 포맷팅"""
    if detail is None:
        return "체결 정보를 조회할 수 없습니다."

    ord_dt = str(detail.get('ord_dt', ''))
    ord_tmd = str(detail.get('ord_tmd', ''))
    if len(ord_tmd) == 6:
        ord_tmd = f"{ord_tmd[:2]}:{ord_tmd[2:4]}:{ord_tmd[4:]}"
    if len(ord_dt) == 8:
        ord_dt = f"{ord_dt[:4]}-{ord_dt[4:6]}-{ord_dt[6:]}"

    avg_price = int(float(detail.get('avg_prvs', 0) or 0))
    tot_qty = detail.get('tot_ccld_qty', '0')
    tot_amt = int(float(detail.get('tot_ccld_amt', 0) or 0))
    prsm_tlex = int(float(detail.get('prsm_tlex_smtl', 0) or 0))

    info = (
        f"주문일시: {ord_dt} {ord_tmd}\n"
        f"매입평균가격: {avg_price:,}원\n"
        f"총체결수량: {tot_qty}주\n"
        f"총체결금액: {tot_amt:,}원\n"
        f"추정제비용: {prsm_tlex:,}원"
    )
    return info

def format_buy_message(buyback_info, execution_detail):
    """매수 체결 텔레그램 메시지 생성"""
    msg = "[매수 체결 알림]\n"
    msg += "=" * 28 + "\n\n"
    msg += "[자사주 매입 정보]\n"
    msg += f"종목명: {buyback_info['corp_name']}\n"
    msg += f"종목코드: {buyback_info['stock_code']}\n"
    msg += f"공시유형: {buyback_info['report_nm']}\n"
    msg += f"매입금액: {buyback_info['amount']:,} 억원\n"
    msg += f"시가총액: {buyback_info['market_cap']:,} 억원\n"
    msg += f"비율: {buyback_info['ratio']}%\n"
    msg += f"매입기간: {buyback_info['start_dt']} ~ {buyback_info['end_dt']}\n"
    msg += f"목적: {buyback_info['purpose']}\n\n"
    msg += "[체결 정보]\n"
    msg += format_execution_info(execution_detail) + "\n\n"
    msg += "=" * 28 + "\n"
    msg += f"공시링크: https://dart.fss.or.kr/dsaf001/main.do?rcpNo={buyback_info['rcept_no']}"
    return msg

def format_sell_message(stock_code, prdt_name, profit_rate, execution_detail):
    """매도 체결 텔레그램 메시지 생성"""
    msg = "[매도 체결 알림]\n"
    msg += "=" * 28 + "\n\n"
    msg += "[매도 정보]\n"
    msg += f"종목코드: {stock_code}\n"
    if prdt_name:
        msg += f"종목명: {prdt_name}\n"
    msg += f"수익률: {profit_rate}%\n\n"
    msg += "[체결 정보]\n"
    msg += format_execution_info(execution_detail) + "\n\n"
    msg += "=" * 28
    return msg

def execute_buy_order(stock_code, buyback_ratio, buyback_info=None):
    """
    자사주 매입 비율(buyback_ratio)을 바탕으로 주문금액과 주문수량을 산정하여 주식 매수를 실행하는 함수입니다.

    매개변수:
      - stock_code: 주식 종목 코드 (예: "A005930")
      - buyback_ratio: 자사주 매입 비율 (예: 2.35 => 정수 내림 후 2% 적용)
      - buyback_info: 자사주 매입 공시 정보 (텔레그램 전송용)

    동작:
      1. 비율을 정수 내림하여 1% 당 10만원의 주문금액(order_amount)을 계산.
      2. 현재가(current_price)를 조회하여 주문수량(order_quantity)를 계산 (order_amount // current_price).
      3. 주문수량이 1 미만이면 주문을 실행하지 않음.
      4. 주문 실행 후 체결 정보를 조회하여 텔레그램으로 전송.
    """
    # 0. 비율 검증 (100% 이상이면 오류로 판단)
    if buyback_ratio >= 100:
        print(f"자사주 매입 비율 오류: {buyback_ratio}% (100% 이상). 주문을 건너뜁니다.")
        return None

    # 1. 비율 정수 내림 및 주문금액 계산
    effective_ratio = int(buyback_ratio)  # 예: 2.35 -> 2
    order_amount = effective_ratio * 100000  # 1% 당 10만원
    order_stock_code = stock_code[1:]

    # 2. 현재가 조회 및 주문수량 계산
    cp = current_price(order_stock_code)  # 기존에 정의된 current_price 함수 사용 (현재가 반환)
    order_quantity = order_amount // cp  # 정수 나눗셈 (주문수량 계산)
    
    if order_quantity < 1:
        print(f"주문수량이 1 미만입니다. 주문을 실행하지 않습니다.")
        return None
    
    # 3. 주문 실행 (buy 함수 호출)
    try:
        result = buy(order_stock_code, order_quantity)
        if result is None:
            return None
    except Exception as e:
        print(f"주문 실행 중 오류 발생: {e}")
        return None  # 오류 발생 시 이후 단계 실행하지 않고 함수 종료

    # 3-1. 체결 정보 조회 후 텔레그램 전송 (매수)
    if buyback_info is not None:
        try:
            execution_detail = get_execution_detail(stock_code)
            msg = format_buy_message(buyback_info, execution_detail)
            asyncio.run(send_message(msg))
        except Exception as e:
            print(f"매수 텔레그램 전송 오류: {e}")
    
# 잔고 조회
def get_balance():
    # [국내주식] 주문/계좌 > 주식잔고조회 (보유종목리스트)
    rt_data = kb.get_inquire_balance_lst()
    selected_data = rt_data[['pdno', 'prdt_name', 'hldg_qty', 'evlu_pfls_rt']]
    return selected_data

# 수익률이 +10% 이상이거나 -5% 이하인 종목은 매도
def execute_sell_order():
    balance = get_balance()
    for _, row in balance.iterrows():
        # 수익률을 숫자로 변환
        profit_rate = float(row['evlu_pfls_rt'])
        
        if profit_rate >= 10 or profit_rate <= -5:
            result = sell(row['pdno'], row['hldg_qty'])
            if result is None:
                continue
            print(f"매도 완료: 종목코드{row['pdno']} 수량{row['hldg_qty']} 수익률{row['evlu_pfls_rt']}")

            # 체결 정보 조회 후 텔레그램 전송 (매도)
            try:
                execution_detail = get_execution_detail(row['pdno'])
                msg = format_sell_message(row['pdno'], row.get('prdt_name', ''), profit_rate, execution_detail)
                asyncio.run(send_message(msg))
            except Exception as e:
                print(f"매도 텔레그램 전송 오류: {e}")
                asyncio.run(send_message(f"매도 완료: 종목코드{row['pdno']} 수량{row['hldg_qty']} 수익률{row['evlu_pfls_rt']}"))

# 함수 실행
if __name__ == "__main__":
    # 프로그램 시작 시 데이터베이스 연결 확인
    print("프로그램 시작 - 데이터베이스 연결 확인 중...")
    check_db_connection()
    
    # 마지막 DB 연결 확인 시간 추적
    last_db_check = datetime.now()
    db_check_interval = timedelta(minutes=30)  # 30분마다 연결 상태 확인
    
    while True:
        now = datetime.now()
        
        # 오후 4시(16:00)에 프로그램 종료
        shutdown_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
        if now >= shutdown_time:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 오후 4시가 되어 프로그램을 종료합니다.")
            break
        
        # 30분마다 데이터베이스 연결 상태 확인
        if now - last_db_check >= db_check_interval:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 정기 데이터베이스 연결 상태 확인...")
            if not check_db_connection():
                print("데이터베이스 연결 실패 - 5분 후 재시도")
                time.sleep(300)  # 5분 대기
                continue
            last_db_check = now
        
        start_time = now.replace(hour=9, minute=0, second=10, microsecond=0)
        end_time = now.replace(hour=15, minute=19, second=30, microsecond=0)

        new_entries = main()  # 새로운 공시 정보를 가져옴
        
        if start_time <= now <= end_time:
            # 매도 주문 처리
            try:
                execute_sell_order()
            except Exception as e:
                print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 매도 주문 처리 중 오류 발생: {e}")
            
            # 매수 주문 처리
            if new_entries is not None and not new_entries.empty:
                for idx, row in new_entries.iterrows():
                    stock_code = None  # 변수 초기화
                    try:
                        # 주식 코드: 'A005930' 형태로 저장되어 있으므로 execute_buy_order 내에서 A 제거 처리함
                        stock_code = row['stock_code']
                        # 'ratio' 컬럼은 자사주 매입 비율(예: 2.35)이 저장되어 있음
                        buyback_ratio = row['ratio']
                        
                        # execute_buy_order 함수를 호출하여 주문 실행 및 주문 내역 DB 기록
                        execute_buy_order(stock_code, buyback_ratio, buyback_info=row)
                        
                    except Exception as e:
                        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 매수 주문 처리 중 오류 발생: {e}")
                        print(f"오류 발생 종목: {stock_code if stock_code else '알 수 없음'}")
                        continue  # 오류 발생 시 다음 종목으로 넘어감
        else:
            print("Out of execution time range. Current time:", now.strftime("%Y-%m-%d %H:%M:%S"))

        time.sleep(2)

        


