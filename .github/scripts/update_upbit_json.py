#!/usr/bin/env python3
"""
업비트 데이터를 JSON 파일로 직접 업데이트
DB 저장 단계를 건너뛰고 바로 JSON 파일에 저장
"""

import os
import requests
import json
import time
from datetime import datetime, timedelta

UPBIT_API_BASE = "https://api.upbit.com/v1"
JSON_FILE_PATH = "src/data/momentum/upbit/upbit_historical_data.json"
TICKERS_FILE_PATH = "src/data/momentum/upbit/upbit_tickers.json"

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

# ✅ 업비트 마켓 리스트
def get_krw_markets():
    url = f"{UPBIT_API_BASE}/market/all"
    res = requests.get(url)
    res.raise_for_status()
    return [m for m in res.json() if m["market"].startswith("KRW-")]

# ✅ 어제 종가 가져오기
def fetch_yesterday_candle(market):
    kst_now = datetime.utcnow() + timedelta(hours=9)
    kst_yesterday_end = (kst_now - timedelta(days=1)).strftime("%Y-%m-%dT23:59:59")
    url = f"{UPBIT_API_BASE}/candles/days"
    params = {"market": market, "count": 1, "to": kst_yesterday_end}
    res = requests.get(url, params=params)
    res.raise_for_status()
    return res.json()[0]

# ✅ JSON 파일 로드
def load_json_data():
    if not os.path.exists(JSON_FILE_PATH):
        return {
            'generated_at': datetime.now().isoformat(),
            'cutoff_date': (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d"),
            'total_tickers': 0,
            'total_records': 0,
            'data': []
        }

    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

# ✅ JSON 파일 저장
def save_json_data(data):
    os.makedirs(os.path.dirname(JSON_FILE_PATH), exist_ok=True)
    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ✅ 티커 목록 저장
def save_tickers(tickers):
    os.makedirs(os.path.dirname(TICKERS_FILE_PATH), exist_ok=True)
    with open(TICKERS_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump({
            'generated_at': datetime.now().isoformat(),
            'tickers': tickers
        }, f, ensure_ascii=False, indent=2)

# ✅ 메인 실행
def main():
    print("🚀 업비트 JSON 업데이트 시작...")

    # 1. 기존 JSON 로드
    json_data = load_json_data()
    ticker_map = {ticker['mcv_id']: ticker for ticker in json_data['data']}

    print(f"📊 기존 데이터: {len(ticker_map)}개 티커")

    # 2. 업비트에서 어제 데이터 가져오기
    markets = get_krw_markets()
    yesterday_date = (datetime.utcnow() + timedelta(hours=9) - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📅 업데이트 날짜: {yesterday_date}")
    print(f"📋 총 {len(markets)}개 마켓 처리 중...")

    updated_count = 0
    new_tickers = []
    processed = 0

    for m in markets:
        processed += 1
        if processed % 50 == 0:
            print(f"   진행: {processed}/{len(markets)} ({processed*100//len(markets)}%)")
        market = m["market"]
        ticker = market.replace("KRW-", "")
        mcv_id = f"{ticker}-KRW-UPBIT"

        try:
            # 어제 캔들 데이터 가져오기
            c = fetch_yesterday_candle(market)
            candle_date = c["candle_date_time_kst"][:10]

            # 새 레코드 생성
            new_record = {
                'date': candle_date,
                'open': c["opening_price"],
                'high': c["high_price"],
                'low': c["low_price"],
                'close': c["trade_price"],
                'volume': c["candle_acc_trade_volume"],
                'rsi': None,
                'ema200_diff': None,
                'ema120_diff': None,
                'ema50_diff': None,
                'ema20_diff': None,
                'volume_ratio_90d': None,
                'volume_ratio_alltime': None
            }

            # 기존 티커가 있으면 업데이트, 없으면 새로 추가
            if mcv_id in ticker_map:
                ticker_data = ticker_map[mcv_id]

                # 중복 날짜 체크
                existing_dates = [h['date'] for h in ticker_data['history']]
                if candle_date not in existing_dates:
                    ticker_data['history'].append(new_record)

                    # 최근 250일 데이터로 지표 계산
                    history = ticker_data['history'][-250:]
                    closes = [h['close'] for h in history]
                    volumes = [h['volume'] for h in history]

                    # RSI 계산
                    rsi = calculate_rsi(closes)

                    # EMA 계산
                    ema20 = calculate_ema(closes, 20)
                    ema50 = calculate_ema(closes, 50)
                    ema120 = calculate_ema(closes, 120)
                    ema200 = calculate_ema(closes, 200)

                    current_price = closes[-1]
                    ema20_diff = round(((current_price - ema20) / ema20) * 100, 2) if ema20 else None
                    ema50_diff = round(((current_price - ema50) / ema50) * 100, 2) if ema50 else None
                    ema120_diff = round(((current_price - ema120) / ema120) * 100, 2) if ema120 else None
                    ema200_diff = round(((current_price - ema200) / ema200) * 100, 2) if ema200 else None

                    # 거래량 비율
                    vol_max_90d = max(volumes[-90:]) if len(volumes) >= 90 else max(volumes)
                    vol_ratio_90d = round(volumes[-1] / vol_max_90d, 3) if vol_max_90d else None
                    vol_max_alltime = max(volumes)
                    vol_ratio_alltime = round(volumes[-1] / vol_max_alltime, 3) if vol_max_alltime else None

                    # 지표 업데이트
                    new_record['rsi'] = rsi
                    new_record['ema20_diff'] = ema20_diff
                    new_record['ema50_diff'] = ema50_diff
                    new_record['ema120_diff'] = ema120_diff
                    new_record['ema200_diff'] = ema200_diff
                    new_record['volume_ratio_90d'] = vol_ratio_90d
                    new_record['volume_ratio_alltime'] = vol_ratio_alltime

                    ticker_data['history'][-1] = new_record
                    updated_count += 1
            else:
                # 신규 티커
                ticker_map[mcv_id] = {
                    'mcv_id': mcv_id,
                    'ticker': ticker,
                    'history': [new_record]
                }
                new_tickers.append({'mcv_id': mcv_id, 'ticker': ticker})
                updated_count += 1

            time.sleep(0.05)  # API rate limit (최적화)

        except Exception as e:
            print(f"❌ {market} 에러: {e}")
            continue

    # 3. JSON 저장
    json_data['data'] = list(ticker_map.values())
    json_data['total_tickers'] = len(ticker_map)
    json_data['total_records'] = sum(len(t['history']) for t in ticker_map.values())
    json_data['generated_at'] = datetime.now().isoformat()

    save_json_data(json_data)

    # 4. 티커 목록 저장
    all_tickers = [{'mcv_id': k, 'ticker': v['ticker']} for k, v in ticker_map.items()]
    save_tickers(all_tickers)

    print(f"\n✅ 업데이트 완료!")
    print(f"   - 총 티커: {len(ticker_map)}")
    print(f"   - 업데이트된 레코드: {updated_count}")
    print(f"   - 신규 티커: {len(new_tickers)}")
    print(f"   - 총 레코드: {json_data['total_records']}")

    file_size = os.path.getsize(JSON_FILE_PATH) / 1024 / 1024
    print(f"   - 파일 크기: {file_size:.2f} MB")

if __name__ == "__main__":
    main()
