#!/usr/bin/env python3
"""
미국 주식/ETF 전체 히스토리 재구축 (2022-01-01부터)
Yahoo Finance에서 데이터 가져와서 JSON 생성

✨ 개선사항:
- 병렬처리: ThreadPoolExecutor로 10개 동시 실행 (기존)
- 재시도: API 실패 시 4회 재시도 (지수 백오프) - 신규
"""

import os
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import yfinance as yf
from utils_common import retry_on_failure, calculate_and_update_indicators

# 경로 설정
JSON_FILE_PATH = "src/data/momentum/us/us_historical_data.json"
TICKERS_FILE_PATH = "src/data/momentum/us/us_tickers.json"

# 로컬 티커 파일 경로 (US 폴더 전체)
TICKER_FILES = [
    # Stocks
    "src/data/tickers/us/stocks/stocks_us_nasdaq100_with_mcv_id.json",
    "src/data/tickers/us/stocks/stocks_us_s&p500_with_mcv_id.json",
    "src/data/tickers/us/stocks/stocks_us_russell2000_with_mcv_id.json",  # ✅ Russell 2000 추가
    # ETF
    "src/data/tickers/us/etf/etf_us_largest_with_mcv_id.json",
    "src/data/tickers/us/etf/etf_us_leverage_2x_with_mcv_id.json",
    "src/data/tickers/us/etf/etf_us_leverage_3x_with_mcv_id.json",
    "src/data/tickers/us/etf/etf_us_others_with_mcv_id.json",
    "src/data/tickers/us/etf/etf_us_popular_with_mcv_id.json",
    # Index
    "src/data/tickers/us/index/index_us_with_mcv_id.json",
    # Commodity
    "src/data/tickers/us/commodity/commodity_with_mcv_id.json",
    # Bond
    "src/data/tickers/us/bond/bond_us_with_mcv_id.json",
    # Forex
    "src/data/tickers/us/forex/forex_us_with_mcv_id.json",
]

# ✅ 로컬 티커 파일 로드
def load_local_tickers():
    all_tickers = []
    for file_path in TICKER_FILES:
        if not os.path.exists(file_path):
            print(f"⚠️ 티커 파일 없음: {file_path}")
            continue

        with open(file_path, 'r', encoding='utf-8') as f:
            tickers = json.load(f)
            all_tickers.extend(tickers)
            print(f"✅ 로드: {file_path} - {len(tickers)}개")

    # 중복 제거 (mcv_id 기준)
    unique_tickers = {}
    for t in all_tickers:
        mcv_id = t.get('mcv_id')
        if mcv_id and mcv_id not in unique_tickers:
            unique_tickers[mcv_id] = t

    return list(unique_tickers.values())

# ✅ Yahoo Finance에서 히스토리 가져오기 (병렬 처리용, 재시도 로직 포함)
@retry_on_failure(max_retries=4)
def fetch_yahoo_history(ticker, start_date, end_date):
    """
    Yahoo Finance에서 OHLCV 데이터 가져오기 (재시도 포함)
    """
    # yfinance가 자동으로 세션 관리 (curl_cffi 사용)
    yf_ticker = yf.Ticker(ticker)
    hist = yf_ticker.history(start=start_date, end=end_date)

    if hist.empty:
        return []

    candles = []
    for date, row in hist.iterrows():
        candles.append({
            'date': date.strftime('%Y-%m-%d'),
            'open': round(float(row['Open']), 2) if row['Open'] else None,
            'high': round(float(row['High']), 2) if row['High'] else None,
            'low': round(float(row['Low']), 2) if row['Low'] else None,
            'close': round(float(row['Close']), 2) if row['Close'] else None,
            'volume': int(row['Volume']) if row['Volume'] else 0,
            'rsi': None,
            'ema200_diff': None,
            'ema120_diff': None,
            'ema50_diff': None,
            'ema20_diff': None,
            'volume_ratio_90d': None,
            'volume_ratio_alltime': None
        })

    time.sleep(0.15)  # Rate limit (증가)
    return candles

# ✅ 단일 티커 처리 함수 (병렬화용)
def process_single_ticker(t, start_date, end_date, index, total):
    """단일 티커 처리 (병렬 실행)"""
    ticker = t.get('ticker')
    mcv_id = t.get('mcv_id')
    ko_name = t.get('ko_name')

    if not ticker or not mcv_id:
        return None, f"⚠️ [{index}/{total}] 잘못된 티커 데이터"

    try:
        # Yahoo Finance에서 데이터 가져오기
        candles = fetch_yahoo_history(ticker, start_date, end_date)

        if len(candles) == 0:
            return None, f"❌ [{index}/{total}] {ticker} 데이터 없음"

        # 히스토리 구축
        history = candles

        # 지표 계산 (utils_common 사용)
        if len(candles) > 0:
            indicators = calculate_and_update_indicators(candles)
            candles[-1].update(indicators)

        return {
            'ticker_data': {
                'mcv_id': mcv_id,
                'ticker': ticker,
                'ko_name': ko_name,
                'history': history
            },
            'ticker_info': {
                'mcv_id': mcv_id,
                'ticker': ticker,
                'ko_name': ko_name
            }
        }, f"✅ [{index}/{total}] {ticker} {len(candles)}개"

    except Exception as e:
        return None, f"❌ [{index}/{total}] {ticker} 에러: {e}"

# ✅ 티커 목록 저장
def save_tickers(tickers):
    os.makedirs(os.path.dirname(TICKERS_FILE_PATH), exist_ok=True)
    with open(TICKERS_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump({
            'generated_at': datetime.now().isoformat(),
            'tickers': tickers
        }, f, ensure_ascii=False, indent=2)

# ✅ JSON 파일 저장
def save_json_data(data):
    os.makedirs(os.path.dirname(JSON_FILE_PATH), exist_ok=True)
    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ✅ 메인 실행 (병렬 처리)
def main():
    print("🚀 미국 주식/ETF 전체 히스토리 재구축 시작 (병렬 처리)")

    start_date = "2022-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"📅 수집 기간: {start_date} ~ {end_date}")

    # 1. 로컬 티커 리스트 가져오기
    tickers = load_local_tickers()
    print(f"📋 총 {len(tickers)}개 티커 처리 중...\n")
    print(f"⚡ 병렬 처리: 5개 스레드 + 재시도 로직 (예상 시간: 3-4시간)\n")

    all_data = []
    all_tickers = []
    failed = 0
    print_lock = Lock()

    # 병렬 처리 (5개 스레드 - Yahoo Finance rate limit 회피)
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 모든 작업을 제출
        future_to_ticker = {
            executor.submit(process_single_ticker, t, start_date, end_date, i+1, len(tickers)): t
            for i, t in enumerate(tickers)
        }

        # 완료된 작업 처리
        for future in as_completed(future_to_ticker):
            result, log_msg = future.result()

            with print_lock:
                print(log_msg)

                if result:
                    all_data.append(result['ticker_data'])
                    all_tickers.append(result['ticker_info'])
                else:
                    failed += 1

                # 진행 상황 (50개마다)
                if len(all_data) % 50 == 0:
                    print(f"   📊 진행: {len(all_data) + failed}/{len(tickers)} ({(len(all_data) + failed)*100//len(tickers)}%)")

    # JSON 저장
    json_data = {
        'generated_at': datetime.now().isoformat(),
        'cutoff_date': start_date,
        'total_tickers': len(all_data),
        'total_records': sum(len(t['history']) for t in all_data),
        'data': all_data
    }

    save_json_data(json_data)
    save_tickers(all_tickers)

    print(f"\n✅ 재구축 완료!")
    print(f"   - 성공: {len(all_data)}개")
    print(f"   - 실패: {failed}개")
    print(f"   - 총 레코드: {json_data['total_records']}")
    print(f"   - 시작일: {start_date}")
    print(f"   - 종료일: {end_date}")

    if os.path.exists(JSON_FILE_PATH):
        file_size = os.path.getsize(JSON_FILE_PATH) / 1024 / 1024
        print(f"   - 파일 크기: {file_size:.2f} MB")

if __name__ == "__main__":
    main()
