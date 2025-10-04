#!/usr/bin/env python3
"""
미국 주식/ETF 전체 히스토리 재구축 (2022-01-01부터)
Yahoo Finance에서 데이터 가져와서 JSON 생성
"""

import os
import json
import time
from datetime import datetime, timedelta
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

# ✅ Yahoo Finance에서 히스토리 가져오기
def fetch_yahoo_history(ticker, start_date, end_date):
    """
    Yahoo Finance에서 OHLCV 데이터 가져오기
    """
    import random
    import requests
    try:
        # Random sleep to avoid rate limiting (3-5초)
        time.sleep(random.uniform(3.0, 5.0))

        # User-Agent 설정으로 차단 우회
        session = requests.Session()
        session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

        yf_ticker = yf.Ticker(ticker, session=session)
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
        print(f"❌ {ticker} 에러: {e}")
        return []

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

# ✅ 메인 실행
def main():
    print("🚀 미국 주식/ETF 전체 히스토리 재구축 시작...")

    start_date = "2022-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"📅 수집 기간: {start_date} ~ {end_date}")

    # 1. 로컬 티커 리스트 가져오기
    tickers = load_local_tickers()
    print(f"📋 총 {len(tickers)}개 티커 처리 중...\n")

    all_data = []
    all_tickers = []
    processed = 0
    failed = 0

    for t in tickers:
        processed += 1
        ticker = t.get('ticker')
        mcv_id = t.get('mcv_id')
        ko_name = t.get('ko_name')

        if not ticker or not mcv_id:
            print(f"⚠️ [{processed}/{len(tickers)}] 잘못된 티커 데이터: {t}")
            failed += 1
            continue

        print(f"   [{processed}/{len(tickers)}] {ticker} 처리 중...", end=" ", flush=True)

        try:
            # Yahoo Finance에서 데이터 가져오기
            candles = fetch_yahoo_history(ticker, start_date, end_date)

            if len(candles) == 0:
                print("❌ 데이터 없음")
                failed += 1
                continue

            print(f"✅ {len(candles)}개")

            # 히스토리 구축 (candles가 이미 완성된 형태)
            history = candles

            # 최근 250일 데이터로 지표 계산
            recent_history = history[-250:]
            closes = [h['close'] for h in recent_history if h['close'] is not None]
            volumes = [h['volume'] for h in recent_history if h['volume'] is not None]

            if len(closes) < 14:  # RSI 계산 최소 데이터
                print(f"⚠️ {ticker} 데이터 부족 (최근 {len(closes)}일)")
                failed += 1
                continue

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

            all_data.append({
                'mcv_id': mcv_id,
                'ticker': ticker,
                'ko_name': ko_name,
                'history': history
            })

            all_tickers.append({
                'mcv_id': mcv_id,
                'ticker': ticker,
                'ko_name': ko_name
            })

            # 진행 상황 표시 (50개마다)
            if processed % 50 == 0:
                print(f"   📊 진행: {processed}/{len(tickers)} ({processed*100//len(tickers)}%)")

            time.sleep(2.0)  # API rate limit (1.0 → 2.0초)

        except Exception as e:
            print(f"❌ {ticker} 에러: {e}")
            failed += 1
            time.sleep(5.0)  # 에러 시 더 길게 대기 (2.0 → 5.0초)
            continue

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
