#!/usr/bin/env python3
"""
CryptoCompare 일일 업데이트 (증분 업데이트)
- 기존 코인: 어제 데이터만 추가
- 신규 코인: 2022-01-01부터 전체 히스토리 다운로드
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

# 신규 코인 최대 처리 개수
MAX_NEW_COINS = 5

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

# ✅ 기존 JSON 데이터 로드
def load_existing_data():
    if not os.path.exists(JSON_FILE_PATH):
        print("⚠️ 기존 JSON 파일 없음 - rebuild 스크립트를 먼저 실행하세요")
        return None

    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

# ✅ CryptoCompare API에서 최근 데이터 가져오기
def fetch_cryptocompare_recent(symbol, days=2):
    """최근 며칠 데이터 가져오기"""
    try:
        params = {
            "fsym": symbol,
            "tsym": "USD",
            "limit": days
        }

        url = f"{CRYPTOCOMPARE_API_BASE}/data/v2/histoday"
        response = requests.get(url, params=params)

        if response.status_code == 429:
            print(f"⚠️ Rate limit - 30초 대기...")
            time.sleep(30)
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

# ✅ 전체 히스토리 가져오기 (신규 코인용)
def fetch_cryptocompare_full_history(symbol, start_date="2022-01-01"):
    """2022-01-01부터 전체 히스토리 가져오기"""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        today = datetime.now()
        days = (today - start).days

        params = {
            "fsym": symbol,
            "tsym": "USD",
            "limit": min(days, 2000),
            "toTs": int(today.timestamp())
        }

        url = f"{CRYPTOCOMPARE_API_BASE}/data/v2/histoday"
        response = requests.get(url, params=params)

        if response.status_code == 429:
            time.sleep(60)
            response = requests.get(url, params=params)

        response.raise_for_status()
        data = response.json()

        if data.get("Response") == "Error":
            return []

        history_data = data.get("Data", {}).get("Data", [])

        candles = []
        for item in history_data:
            date = datetime.fromtimestamp(item["time"]).strftime('%Y-%m-%d')

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

# ✅ 지표 계산 및 업데이트
def calculate_and_update_indicators(history):
    """최신 레코드에 RSI, EMA, 거래량비율 계산"""
    if len(history) == 0:
        return

    recent_history = history[-250:]
    closes = [h['close'] for h in recent_history if h['close'] is not None]
    volumes = [h['volume'] for h in recent_history if h['volume'] is not None]

    if len(closes) < 14:
        return

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

    # 최신 레코드 업데이트
    history[-1]['rsi'] = rsi
    history[-1]['ema20_diff'] = ema20_diff
    history[-1]['ema50_diff'] = ema50_diff
    history[-1]['ema120_diff'] = ema120_diff
    history[-1]['ema200_diff'] = ema200_diff
    history[-1]['volume_ratio_90d'] = vol_ratio_90d
    history[-1]['volume_ratio_alltime'] = vol_ratio_alltime

# ✅ JSON 파일 저장
def save_json_data(data):
    os.makedirs(os.path.dirname(JSON_FILE_PATH), exist_ok=True)
    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ✅ 메인 실행
def main():
    print("🔄 CryptoCompare 일일 업데이트 시작...")

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 업데이트 날짜: {yesterday}\n")

    # 1. CryptoCompare API로 최신 상위 1000개 코인 가져오기
    live_tickers = fetch_top_coins()
    print(f"📋 최신 상위 코인: {len(live_tickers)}개")

    # 2. 기존 JSON 데이터 로드
    existing_data = load_existing_data()
    if not existing_data:
        print("❌ 기존 데이터 없음 - rebuild_cryptocompare_history.py를 먼저 실행하세요")
        return

    # 기존 코인 맵 생성
    existing_map = {item['mcv_id']: item for item in existing_data['data']}
    existing_mcv_ids = set(existing_map.keys())

    # 3. 신규 코인 vs 기존 코인 분리
    new_coins = [t for t in live_tickers if t['mcv_id'] not in existing_mcv_ids]
    existing_coins = [t for t in live_tickers if t['mcv_id'] in existing_mcv_ids]

    # 순위권 밖 코인 (히스토리 유지)
    live_mcv_ids = {t['mcv_id'] for t in live_tickers}
    dropped_coins = [item for item in existing_data['data'] if item['mcv_id'] not in live_mcv_ids]

    print(f"🆕 신규 진입 코인: {len(new_coins)}개")
    print(f"🔄 기존 상위 코인: {len(existing_coins)}개")
    print(f"📉 순위권 밖 코인: {len(dropped_coins)}개 (히스토리 유지)\n")

    # 4. 기존 코인 업데이트 (최근 2일 데이터 추가)
    updated_count = 0
    print("📊 기존 코인 업데이트 중...")
    for t in existing_coins:
        symbol = t.get('ticker')
        mcv_id = t['mcv_id']

        try:
            candles = fetch_cryptocompare_recent(symbol, days=2)

            if len(candles) == 0:
                continue

            coin_data = existing_map[mcv_id]

            # 중복 체크
            existing_dates = {h['date'] for h in coin_data['history']}
            for candle in candles:
                if candle['date'] not in existing_dates:
                    coin_data['history'].append(candle)

            # 지표 재계산
            calculate_and_update_indicators(coin_data['history'])
            updated_count += 1

            if updated_count % 50 == 0:
                print(f"   진행: {updated_count}/{len(existing_coins)}")

        except Exception as e:
            print(f"❌ {symbol} 업데이트 실패: {e}")
            continue

        time.sleep(1)

    print(f"✅ 기존 코인 업데이트 완료: {updated_count}개\n")

    # 5. 신규 코인 처리
    if len(new_coins) > 0:
        print(f"🆕 신규 코인 처리 중...")

        if len(new_coins) > MAX_NEW_COINS:
            print(f"⚠️ 신규 코인 {len(new_coins)}개 감지 - 오늘은 {MAX_NEW_COINS}개만 처리")
            process_new = new_coins[:MAX_NEW_COINS]
        else:
            process_new = new_coins

        for t in process_new:
            symbol = t.get('ticker')
            mcv_id = t['mcv_id']
            name = t.get('name')

            print(f"   🆕 {symbol} - 2022-01-01부터 다운로드 중...", end=" ", flush=True)

            try:
                candles = fetch_cryptocompare_full_history(symbol)

                if len(candles) == 0:
                    print("❌ 데이터 없음")
                    continue

                print(f"✅ {len(candles)}개")

                # 지표 계산
                calculate_and_update_indicators(candles)

                # 데이터에 추가
                existing_data['data'].append({
                    'mcv_id': mcv_id,
                    'ticker': symbol,
                    'name': name,
                    'history': candles
                })

                time.sleep(3)

            except Exception as e:
                print(f"❌ {symbol} 다운로드 실패: {e}")
                continue

        print(f"✅ 신규 코인 처리 완료: {len(process_new)}개\n")

    # 6. 메타데이터 업데이트
    existing_data['generated_at'] = datetime.now().isoformat()
    existing_data['total_tickers'] = len(existing_data['data'])
    existing_data['total_records'] = sum(len(t['history']) for t in existing_data['data'])

    # 7. JSON 저장
    save_json_data(existing_data)

    print("✅ 일일 업데이트 완료!")
    print(f"   - 총 코인: {existing_data['total_tickers']}개")
    print(f"   - 총 레코드: {existing_data['total_records']}")
    print(f"   - 업데이트 시각: {existing_data['generated_at']}")

    if os.path.exists(JSON_FILE_PATH):
        file_size = os.path.getsize(JSON_FILE_PATH) / 1024 / 1024
        print(f"   - 파일 크기: {file_size:.2f} MB")

if __name__ == "__main__":
    main()
