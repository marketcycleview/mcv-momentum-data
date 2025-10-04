#!/usr/bin/env python3
"""
CryptoCompare 전체 히스토리 재구축 (2022-01-01부터)
CryptoCompare API에서 데이터 가져와서 JSON 생성
"""

import os
import json
import time
from datetime import datetime, timedelta
import requests

# 경로 설정
JSON_FILE_PATH = "src/data/momentum/cryptocompare/cryptocompare_historical_data.json"
TICKERS_FILE_PATH = "src/data/momentum/cryptocompare/cryptocompare_tickers.json"

# CryptoCompare API
CRYPTOCOMPARE_API_BASE = "https://min-api.cryptocompare.com"

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

# ✅ CryptoCompare API에서 최신 상위 1000개 코인 가져오기
def fetch_top_coins():
    """CryptoCompare API로 시가총액 상위 1000개 코인 목록 가져오기"""
    print("📡 CryptoCompare API로 최신 상위 1000개 코인 목록 가져오는 중...")

    all_coins = []
    for page in range(10):  # 100 * 10 = 1000개
        print(f"   페이지 {page + 1}/10 수집 중...", end=" ", flush=True)

        params = {
            "limit": 100,
            "tsym": "USD",
            "page": page
        }

        try:
            response = requests.get(f"{CRYPTOCOMPARE_API_BASE}/data/top/mktcapfull", params=params)

            if response.status_code == 429:
                print("⚠️ Rate limit - 60초 대기...")
                time.sleep(60)
                response = requests.get(f"{CRYPTOCOMPARE_API_BASE}/data/top/mktcapfull", params=params)

            response.raise_for_status()
            data = response.json()

            if data.get("Response") == "Error":
                print(f"❌ API 에러: {data.get('Message')}")
                break

            coins = data.get("Data", [])
            all_coins.extend(coins)
            print(f"✅ {len(coins)}개")

            if page < 9:
                time.sleep(1)  # Rate limit 대응

        except Exception as e:
            print(f"❌ 페이지 {page + 1} 수집 실패: {e}")
            continue

    # 티커 데이터 정규화
    tickers = []
    for coin_data in all_coins:
        coin_info = coin_data.get("CoinInfo", {})
        symbol = coin_info.get("Name", "")
        name = coin_info.get("FullName", "")

        raw_data = coin_data.get("RAW", {}).get("USD", {})

        common_ticker = f"{symbol}-USD"
        mcv_id = f"{common_ticker}-CRYPTOCOMPARE"

        tickers.append({
            "ticker": symbol,
            "name": name,
            "common_ticker": common_ticker,
            "mcv_id": mcv_id,
            "marketcap": raw_data.get("MKTCAP"),
            "rank": len(tickers) + 1
        })

    print(f"✅ 총 {len(tickers)}개 코인 목록 수집 완료\n")
    return tickers

# ✅ CryptoCompare API에서 히스토리 가져오기
def fetch_cryptocompare_history(symbol, start_date):
    """
    CryptoCompare API에서 일봉 OHLC 데이터 가져오기
    """
    import random
    try:
        # Random sleep to avoid rate limiting (2-3초)
        time.sleep(random.uniform(2.0, 3.0))

        # 시작일부터 오늘까지 일수 계산
        start = datetime.strptime(start_date, "%Y-%m-%d")
        today = datetime.now()
        days = (today - start).days

        params = {
            "fsym": symbol,
            "tsym": "USD",
            "limit": min(days, 2000),  # 최대 2000일
            "toTs": int(today.timestamp())
        }

        url = f"{CRYPTOCOMPARE_API_BASE}/data/v2/histoday"
        response = requests.get(url, params=params)

        if response.status_code == 429:
            print(f"⚠️ Rate limit - 60초 대기...")
            time.sleep(60)
            response = requests.get(url, params=params)

        response.raise_for_status()
        data = response.json()

        if data.get("Response") == "Error":
            return []

        history_data = data.get("Data", {}).get("Data", [])

        if not history_data:
            return []

        candles = []
        for item in history_data:
            date = datetime.fromtimestamp(item["time"]).strftime('%Y-%m-%d')

            # start_date 이후 데이터만
            if date < start_date:
                continue

            candles.append({
                'date': date,
                'open': round(float(item['open']), 8) if item['open'] else None,
                'high': round(float(item['high']), 8) if item['high'] else None,
                'low': round(float(item['low']), 8) if item['low'] else None,
                'close': round(float(item['close']), 8) if item['close'] else None,
                'volume': int(item['volumeto']) if item.get('volumeto') else 0,
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
        print(f"❌ {symbol} 에러: {e}")
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
    print("🚀 CryptoCompare 전체 히스토리 재구축 시작...")

    start_date = "2022-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"📅 수집 기간: {start_date} ~ {end_date}")

    # 1. CryptoCompare API로 최신 상위 1000개 코인 가져오기
    tickers = fetch_top_coins()
    print(f"📋 총 {len(tickers)}개 티커 처리 중...\n")

    all_data = []
    all_tickers = []
    processed = 0
    failed = 0

    for t in tickers:
        processed += 1
        symbol = t.get('ticker')
        mcv_id = t.get('mcv_id')
        name = t.get('name')

        if not symbol or not mcv_id:
            print(f"⚠️ [{processed}/{len(tickers)}] 잘못된 티커 데이터: {t}")
            failed += 1
            continue

        print(f"   [{processed}/{len(tickers)}] {symbol} 처리 중...", end=" ", flush=True)

        try:
            # CryptoCompare API에서 데이터 가져오기
            candles = fetch_cryptocompare_history(symbol, start_date)

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
            volumes = [h['volume'] for h in recent_history if h['volume'] is not None]

            if len(closes) < 14:
                print(f"⚠️ {symbol} 데이터 부족 (최근 {len(closes)}일)")
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
                'ticker': symbol,
                'name': name,
                'history': history
            })

            all_tickers.append({
                'mcv_id': mcv_id,
                'ticker': symbol,
                'name': name
            })

            # 진행 상황 표시 (50개마다)
            if processed % 50 == 0:
                print(f"   📊 진행: {processed}/{len(tickers)} ({processed*100//len(tickers)}%)")

        except Exception as e:
            print(f"❌ {symbol} 에러: {e}")
            failed += 1
            time.sleep(10.0)
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
