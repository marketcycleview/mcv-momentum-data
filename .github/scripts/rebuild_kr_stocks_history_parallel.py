#!/usr/bin/env python3
"""
미국 주식/ETF 전체 히스토리 재구축 (2022-01-01부터)
Yahoo Finance에서 데이터 가져와서 JSON 생성
병렬 처리로 5시간 → 1-2시간으로 단축
"""

import os
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import yfinance as yf

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

# ✅ RSI 계산 (Wilder 방식)
def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return None

    gains = []
    losses = []
    for i in range(1, period + 1):
        change = prices[i] - prices[i - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    for i in range(period + 1, len(prices)):
        change = prices[i] - prices[i - 1]
        gain = max(change, 0)
        loss = abs(min(change, 0))
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

# ✅ EMA 계산
def calculate_ema(prices, period):
    if len(prices) < 1:
        return None
    k = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = price * k + ema * (1 - k)
    return round(ema, 8)

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

# ✅ Yahoo Finance에서 히스토리 가져오기 (병렬 처리용)
def fetch_yahoo_history(ticker, start_date, end_date):
    """
    Yahoo Finance에서 OHLCV 데이터 가져오기
    병렬 처리시 sleep 제거 (rate limit은 ThreadPool 크기로 제어)
    """
    try:
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

        return candles

    except Exception as e:
        return []  # 조용히 실패 (메인 스레드에서 로깅)

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

        # 최근 250일 데이터로 지표 계산
        recent_history = history[-250:]
        closes = [h['close'] for h in recent_history if h['close'] is not None]
        volumes = [h['volume'] for h in recent_history if h['volume'] is not None]

        if len(closes) < 14:
            return None, f"⚠️ [{index}/{total}] {ticker} 데이터 부족"

        # RSI 계산
        rsi = calculate_rsi(closes)

        # EMA 계산
        ema20 = calculate_ema(closes, 20) if len(closes) >= 20 else None
        ema50 = calculate_ema(closes, 50) if len(closes) >= 50 else None
        ema120 = calculate_ema(closes, 120) if len(closes) >= 120 else None
        ema200 = calculate_ema(closes, 200) if len(closes) >= 200 else None

        current_price = closes[-1]
        ema20_diff = round(((current_price - ema20) / ema20) * 100, 2) if ema20 else None
        ema50_diff = round(((current_price - ema50) / ema50) * 100, 2) if ema50 else None
        ema120_diff = round(((current_price - ema120) / ema120) * 100, 2) if ema120 else None
        ema200_diff = round(((current_price - ema200) / ema200) * 100, 2) if ema200 else None

        # 거래량 비율
        if len(volumes) > 0:
            vol_max_90d = max(volumes[-90:]) if len(volumes) >= 90 else max(volumes)
            vol_ratio_90d = round(volumes[-1] / vol_max_90d, 3) if vol_max_90d else None
            vol_max_alltime = max(volumes)
            vol_ratio_alltime = round(volumes[-1] / vol_max_alltime, 3) if vol_max_alltime else None
        else:
            vol_ratio_90d = None
            vol_ratio_alltime = None

        # 최신 레코드에 지표 업데이트
        history[-1]['rsi'] = rsi
        history[-1]['ema20_diff'] = ema20_diff
        history[-1]['ema50_diff'] = ema50_diff
        history[-1]['ema120_diff'] = ema120_diff
        history[-1]['ema200_diff'] = ema200_diff
        history[-1]['volume_ratio_90d'] = vol_ratio_90d
        history[-1]['volume_ratio_alltime'] = vol_ratio_alltime

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
    print(f"⚡ 병렬 처리: 20개 스레드 (예상 시간: 1-2시간)\n")

    all_data = []
    all_tickers = []
    failed = 0
    print_lock = Lock()

    # 병렬 처리 (20개 스레드)
    with ThreadPoolExecutor(max_workers=20) as executor:
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
