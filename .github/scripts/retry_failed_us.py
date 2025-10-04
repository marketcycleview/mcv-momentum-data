#!/usr/bin/env python3
"""
실패한 미국 주식/ETF 재시도 스크립트
기존 JSON에 누락된 티커만 추가 처리 (순차 처리, sleep 포함)
"""

import os
import json
import time
from datetime import datetime
import yfinance as yf

# 경로 설정
JSON_FILE_PATH = "src/data/momentum/us/us_historical_data.json"
TICKERS_FILE_PATH = "src/data/momentum/us/us_tickers.json"

# 로컬 티커 파일 경로
TICKER_FILES = [
    "src/data/tickers/us/stocks/stocks_us_nasdaq100_with_mcv_id.json",
    "src/data/tickers/us/stocks/stocks_us_s&p500_with_mcv_id.json",
    "src/data/tickers/us/stocks/stocks_us_russell2000_with_mcv_id.json",
    "src/data/tickers/us/etf/etf_us_largest_with_mcv_id.json",
    "src/data/tickers/us/etf/etf_us_leverage_2x_with_mcv_id.json",
    "src/data/tickers/us/etf/etf_us_leverage_3x_with_mcv_id.json",
    "src/data/tickers/us/etf/etf_us_others_with_mcv_id.json",
    "src/data/tickers/us/etf/etf_us_popular_with_mcv_id.json",
    "src/data/tickers/us/index/index_us_with_mcv_id.json",
    "src/data/tickers/us/commodity/commodity_with_mcv_id.json",
    "src/data/tickers/us/bond/bond_us_with_mcv_id.json",
    "src/data/tickers/us/forex/forex_us_with_mcv_id.json",
]

# RSI 계산
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

# EMA 계산
def calculate_ema(prices, period):
    if len(prices) < 1:
        return None
    k = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = price * k + ema * (1 - k)
    return round(ema, 8)

# 로컬 티커 파일 로드
def load_local_tickers():
    all_tickers = []
    for file_path in TICKER_FILES:
        if not os.path.exists(file_path):
            continue
        with open(file_path, 'r', encoding='utf-8') as f:
            tickers = json.load(f)
            all_tickers.extend(tickers)
    unique_tickers = {}
    for t in all_tickers:
        mcv_id = t.get('mcv_id')
        if mcv_id and mcv_id not in unique_tickers:
            unique_tickers[mcv_id] = t
    return list(unique_tickers.values())

# Yahoo Finance에서 히스토리 가져오기 (순차 처리, sleep 포함)
def fetch_yahoo_history(ticker, start_date, end_date):
    try:
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
        print(f"   ⚠️ {ticker} 에러: {str(e)[:100]}")
        return []

def main():
    print("🔄 실패한 티커 재시도 시작\n")

    start_date = "2022-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    # 1. 기존 JSON 로드
    if not os.path.exists(JSON_FILE_PATH):
        print("❌ 기존 JSON 파일이 없습니다")
        return

    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        existing_data = json.load(f)

    existing_tickers = {item['ticker'] for item in existing_data['data']}
    print(f"📊 기존 성공: {len(existing_tickers)}개")

    # 2. 전체 티커 리스트
    all_tickers = load_local_tickers()
    print(f"📋 전체 티커: {len(all_tickers)}개\n")

    # 3. 실패한 티커 찾기
    failed_tickers = [t for t in all_tickers if t.get('ticker') not in existing_tickers]
    print(f"❌ 실패한 티커: {len(failed_tickers)}개\n")

    if len(failed_tickers) == 0:
        print("✅ 모든 티커가 이미 처리되었습니다!")
        return

    # 4. 실패한 티커 재시도 (순차 처리, 5초 sleep)
    new_data = []
    new_tickers = []
    success = 0

    for i, t in enumerate(failed_tickers, 1):
        ticker = t.get('ticker')
        mcv_id = t.get('mcv_id')
        ko_name = t.get('ko_name')

        if not ticker or not mcv_id:
            continue

        print(f"[{i}/{len(failed_tickers)}] {ticker} 시도 중...", end=" ")

        candles = fetch_yahoo_history(ticker, start_date, end_date)

        if len(candles) == 0:
            print("❌ 실패")
            continue

        # 지표 계산
        recent_history = candles[-250:]
        closes = [h['close'] for h in recent_history if h['close'] is not None]
        volumes = [h['volume'] for h in recent_history if h['volume'] is not None]

        if len(closes) < 14:
            print("⚠️ 데이터 부족")
            continue

        rsi = calculate_rsi(closes)
        ema20 = calculate_ema(closes, 20) if len(closes) >= 20 else None
        ema50 = calculate_ema(closes, 50) if len(closes) >= 50 else None
        ema120 = calculate_ema(closes, 120) if len(closes) >= 120 else None
        ema200 = calculate_ema(closes, 200) if len(closes) >= 200 else None

        current_price = closes[-1]
        ema20_diff = round(((current_price - ema20) / ema20) * 100, 2) if ema20 else None
        ema50_diff = round(((current_price - ema50) / ema50) * 100, 2) if ema50 else None
        ema120_diff = round(((current_price - ema120) / ema120) * 100, 2) if ema120 else None
        ema200_diff = round(((current_price - ema200) / ema200) * 100, 2) if ema200 else None

        if len(volumes) > 0:
            vol_max_90d = max(volumes[-90:]) if len(volumes) >= 90 else max(volumes)
            vol_ratio_90d = round(volumes[-1] / vol_max_90d, 3) if vol_max_90d else None
            vol_max_alltime = max(volumes)
            vol_ratio_alltime = round(volumes[-1] / vol_max_alltime, 3) if vol_max_alltime else None
        else:
            vol_ratio_90d = None
            vol_ratio_alltime = None

        candles[-1]['rsi'] = rsi
        candles[-1]['ema20_diff'] = ema20_diff
        candles[-1]['ema50_diff'] = ema50_diff
        candles[-1]['ema120_diff'] = ema120_diff
        candles[-1]['ema200_diff'] = ema200_diff
        candles[-1]['volume_ratio_90d'] = vol_ratio_90d
        candles[-1]['volume_ratio_alltime'] = vol_ratio_alltime

        new_data.append({
            'mcv_id': mcv_id,
            'ticker': ticker,
            'ko_name': ko_name,
            'history': candles
        })

        new_tickers.append({
            'mcv_id': mcv_id,
            'ticker': ticker,
            'ko_name': ko_name
        })

        success += 1
        print(f"✅ {len(candles)}개")

        # 5초 대기 (rate limit 회피)
        if i < len(failed_tickers):
            time.sleep(5)

    # 5. 기존 데이터와 병합
    existing_data['data'].extend(new_data)
    existing_data['total_tickers'] = len(existing_data['data'])
    existing_data['total_records'] = sum(len(t['history']) for t in existing_data['data'])
    existing_data['generated_at'] = datetime.now().isoformat()

    # 6. 저장
    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=2)

    # 티커 목록도 업데이트
    if os.path.exists(TICKERS_FILE_PATH):
        with open(TICKERS_FILE_PATH, 'r', encoding='utf-8') as f:
            tickers_data = json.load(f)
        tickers_data['tickers'].extend(new_tickers)
        tickers_data['generated_at'] = datetime.now().isoformat()
        with open(TICKERS_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(tickers_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 재시도 완료!")
    print(f"   - 신규 성공: {success}개")
    print(f"   - 여전히 실패: {len(failed_tickers) - success}개")
    print(f"   - 전체 티커: {existing_data['total_tickers']}개")
    print(f"   - 전체 레코드: {existing_data['total_records']}개")

    if os.path.exists(JSON_FILE_PATH):
        file_size = os.path.getsize(JSON_FILE_PATH) / 1024 / 1024
        print(f"   - 파일 크기: {file_size:.2f} MB")

if __name__ == "__main__":
    main()
