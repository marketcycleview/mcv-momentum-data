#!/usr/bin/env python3
"""
업비트 전체 히스토리 재구축 (2022-01-01부터)
업비트 API에서 직접 가져와서 JSON 생성
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

# ✅ 특정 기간의 캔들 데이터 가져오기 (최대 200개씩)
def fetch_candles(market, start_date, end_date):
    """
    start_date부터 end_date까지의 일봉 데이터 수집
    업비트 API는 최대 200개씩만 반환하므로 페이징 필요
    """
    all_candles = []
    current_end = end_date
    retry_count = 0
    max_retries = 3
    max_iterations = 20  # 최대 20번 반복 (무한루프 방지)

    for iteration in range(max_iterations):
        try:
            url = f"{UPBIT_API_BASE}/candles/days"
            params = {
                "market": market,
                "count": 200,
                "to": f"{current_end}T23:59:59"
            }

            res = requests.get(url, params=params, timeout=10)
            res.raise_for_status()
            candles = res.json()

            if not candles:
                break

            all_candles.extend(candles)

            # 가장 오래된 캔들의 날짜
            oldest_date = candles[-1]["candle_date_time_kst"][:10]

            # start_date 이전이거나, 200개 미만이면 마지막 페이지
            if oldest_date <= start_date or len(candles) < 200:
                break

            # 다음 페이지를 위해 current_end 업데이트
            prev_end = current_end
            current_end = oldest_date

            # 같은 날짜 반복되면 중단 (무한루프 방지)
            if prev_end == current_end:
                break

            time.sleep(0.15)  # API rate limit
            retry_count = 0  # 성공 시 재시도 카운터 리셋

        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"\n⚠️ {market} API 호출 실패 - 현재까지 {len(all_candles)}개 수집")
                break

            time.sleep(1.0 * retry_count)
            continue

    # start_date 이후 데이터만 필터링 & 날짜 순 정렬
    filtered = [c for c in all_candles if c["candle_date_time_kst"][:10] >= start_date]
    filtered.sort(key=lambda x: x["candle_date_time_kst"])

    return filtered

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
    print("🚀 업비트 전체 히스토리 재구축 시작...")

    start_date = "2022-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"📅 수집 기간: {start_date} ~ {end_date}")

    # 1. 마켓 리스트 가져오기
    markets = get_krw_markets()
    print(f"📋 총 {len(markets)}개 마켓 처리 중...")

    all_data = []
    all_tickers = []
    processed = 0

    for m in markets:
        processed += 1
        market = m["market"]
        ticker = market.replace("KRW-", "")
        mcv_id = f"{ticker}-KRW-UPBIT"

        print(f"   [{processed}/{len(markets)}] {ticker} 처리 중...", end=" ", flush=True)

        try:
            # 전체 기간 데이터 가져오기
            candles = fetch_candles(market, start_date, end_date)

            if len(candles) == 0:
                print("❌ 데이터 없음 (스킵)")
                continue

            print(f"✅ {len(candles)}개")

            # 히스토리 구축
            history = []
            for c in candles:
                candle_date = c["candle_date_time_kst"][:10]
                history.append({
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
                })

            # 최근 250일 데이터로 지표 계산
            recent_history = history[-250:]
            closes = [h['close'] for h in recent_history]
            volumes = [h['volume'] for h in recent_history]

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
                'history': history
            })

            all_tickers.append({'mcv_id': mcv_id, 'ticker': ticker})

            # 진행 상황 표시 (50개마다)
            if processed % 50 == 0:
                print(f"\n   📊 진행: {processed}/{len(markets)} ({processed*100//len(markets)}%)")

            time.sleep(0.15)  # API rate limit (0.1 → 0.15초로 증가)

        except Exception as e:
            print(f"❌ {market} 에러 (스킵): {e}")
            time.sleep(0.5)  # 에러 시 더 길게 대기
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
    failed = len(markets) - len(all_data)
    print(f"   - 성공: {len(all_data)}개")
    print(f"   - 실패: {failed}개")
    print(f"   - 총 레코드: {json_data['total_records']}")
    print(f"   - 시작일: {start_date}")
    print(f"   - 종료일: {end_date}")

    file_size = os.path.getsize(JSON_FILE_PATH) / 1024 / 1024
    print(f"   - 파일 크기: {file_size:.2f} MB")

if __name__ == "__main__":
    main()
