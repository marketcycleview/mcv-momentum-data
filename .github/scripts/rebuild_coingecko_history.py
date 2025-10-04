#!/usr/bin/env python3
"""
코인게코 전체 히스토리 재구축 (2022-01-01부터)
CoinGecko API에서 데이터 가져와서 JSON 생성
"""

import os
import json
import time
from datetime import datetime
import requests

# 경로 설정
JSON_FILE_PATH = "src/data/momentum/coingecko/coingecko_historical_data.json"
TICKERS_FILE_PATH = "src/data/momentum/coingecko/coingecko_tickers.json"

# 로컬 티커 파일 경로
TICKER_FILE = "src/data/tickers/crypto/coingecko_with_mcv_id.json"

# CoinGecko API
COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"

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

# ✅ CoinGecko API에서 최신 상위 1000개 코인 가져오기
def fetch_top_coins():
    """CoinGecko API로 시가총액 상위 1000개 코인 목록 가져오기"""
    print("📡 CoinGecko API로 최신 상위 1000개 코인 목록 가져오는 중...")

    all_coins = []
    for page in range(1, 5):  # 250 * 4 = 1000개
        print(f"   페이지 {page}/4 수집 중...", end=" ", flush=True)

        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
            "sparkline": False
        }

        try:
            response = requests.get(f"{COINGECKO_API_BASE}/coins/markets", params=params)

            if response.status_code == 429:
                print("⚠️ Rate limit - 60초 대기...")
                time.sleep(60)
                response = requests.get(f"{COINGECKO_API_BASE}/coins/markets", params=params)

            response.raise_for_status()
            coins = response.json()
            all_coins.extend(coins)
            print(f"✅ {len(coins)}개")

            if page < 4:
                time.sleep(2)  # Rate limit 대응

        except Exception as e:
            print(f"❌ 페이지 {page} 수집 실패: {e}")
            continue

    # 티커 데이터 정규화
    tickers = []
    for coin in all_coins:
        ticker = coin["symbol"].upper()
        name = coin["name"]
        coingecko_id = coin["id"]
        common_ticker = f"{ticker}-USD"
        mcv_id = f"{common_ticker}-COINGECKO"

        tickers.append({
            "ticker": ticker,
            "name": name,
            "coingecko_id": coingecko_id,
            "common_ticker": common_ticker,
            "mcv_id": mcv_id,
            "marketcap": coin.get("market_cap"),
            "rank": coin.get("market_cap_rank")
        })

    print(f"✅ 총 {len(tickers)}개 코인 목록 수집 완료\n")
    return tickers

# ✅ CoinGecko API에서 히스토리 가져오기
def fetch_coingecko_history(coingecko_id, days=1095):
    """
    CoinGecko API에서 OHLC 데이터 가져오기 (최대 3년: 1095일)
    """
    import random
    try:
        # Random sleep to avoid rate limiting (15-20초)
        time.sleep(random.uniform(15.0, 20.0))

        url = f"{COINGECKO_API_BASE}/coins/{coingecko_id}/ohlc"
        params = {
            'vs_currency': 'usd',
            'days': days
        }

        response = requests.get(url, params=params)

        if response.status_code == 429:
            print(f"⚠️ Rate limit - 60초 대기...")
            time.sleep(60)
            response = requests.get(url, params=params)

        response.raise_for_status()
        data = response.json()

        if not data:
            return []

        candles = []
        for ohlc in data:
            timestamp = ohlc[0]
            date = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')

            candles.append({
                'date': date,
                'open': round(float(ohlc[1]), 8) if ohlc[1] else None,
                'high': round(float(ohlc[2]), 8) if ohlc[2] else None,
                'low': round(float(ohlc[3]), 8) if ohlc[3] else None,
                'close': round(float(ohlc[4]), 8) if ohlc[4] else None,
                'volume': None,  # CoinGecko OHLC에는 volume 없음
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
        print(f"❌ {coingecko_id} 에러: {e}")
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
    print("🚀 코인게코 전체 히스토리 재구축 시작...")

    # CoinGecko API는 최대 365일까지만 지원 (무료 플랜)
    # 1095일 = 3년치 요청하지만, 실제로는 365일만 반환됨
    days = 365

    print(f"📅 수집 기간: 최근 {days}일")

    # 1. CoinGecko API로 최신 상위 1000개 코인 가져오기
    tickers = fetch_top_coins()
    print(f"📋 총 {len(tickers)}개 티커 처리 중...\n")

    all_data = []
    all_tickers = []
    processed = 0
    failed = 0

    for t in tickers:
        processed += 1
        coingecko_id = t.get('coingecko_id')
        mcv_id = t.get('mcv_id')
        ticker = t.get('ticker')
        name = t.get('name')

        if not coingecko_id or not mcv_id:
            print(f"⚠️ [{processed}/{len(tickers)}] 잘못된 티커 데이터: {t}")
            failed += 1
            continue

        print(f"   [{processed}/{len(tickers)}] {ticker} ({coingecko_id}) 처리 중...", end=" ", flush=True)

        try:
            # CoinGecko API에서 데이터 가져오기
            candles = fetch_coingecko_history(coingecko_id, days)

            if len(candles) == 0:
                print("❌ 데이터 없음")
                failed += 1
                continue

            print(f"✅ {len(candles)}개")

            # 히스토리 구축
            history = candles

            # 최근 250일 데이터로 지표 계산
            recent_history = history[-250:]
            closes = [h['close'] for h in recent_history if h['close'] is not None]

            if len(closes) < 14:
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

            # 최신 레코드에 지표 업데이트
            history[-1]['rsi'] = rsi
            history[-1]['ema20_diff'] = ema20_diff
            history[-1]['ema50_diff'] = ema50_diff
            history[-1]['ema120_diff'] = ema120_diff
            history[-1]['ema200_diff'] = ema200_diff

            all_data.append({
                'mcv_id': mcv_id,
                'ticker': ticker,
                'name': name,
                'coingecko_id': coingecko_id,
                'history': history
            })

            all_tickers.append({
                'mcv_id': mcv_id,
                'ticker': ticker,
                'name': name,
                'coingecko_id': coingecko_id
            })

            # 진행 상황 표시 (50개마다)
            if processed % 50 == 0:
                print(f"   📊 진행: {processed}/{len(tickers)} ({processed*100//len(tickers)}%)")

        except Exception as e:
            print(f"❌ {ticker} 에러: {e}")
            failed += 1
            time.sleep(30.0)
            continue

    # JSON 저장
    json_data = {
        'generated_at': datetime.now().isoformat(),
        'cutoff_date': f"최근 {days}일",
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
    print(f"   - 수집 기간: 최근 {days}일")

    if os.path.exists(JSON_FILE_PATH):
        file_size = os.path.getsize(JSON_FILE_PATH) / 1024 / 1024
        print(f"   - 파일 크기: {file_size:.2f} MB")

if __name__ == "__main__":
    main()
