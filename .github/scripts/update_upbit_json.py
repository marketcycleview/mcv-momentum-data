#!/usr/bin/env python3
"""
업비트 데이터를 JSON 파일로 직접 업데이트
DB 저장 단계를 건너뛰고 바로 JSON 파일에 저장

✨ 개선사항:
- upsert: 같은 날짜 데이터 덮어쓰기
- 병렬처리: ThreadPoolExecutor로 10개 동시 실행
- 재시도: API 실패 시 4회 재시도 (지수 백오프)
"""

import os
import requests
import json
import time
from datetime import datetime, timedelta
from utils_common import retry_on_failure, parallel_process, upsert_history, calculate_and_update_indicators

UPBIT_API_BASE = "https://api.upbit.com/v1"
JSON_FILE_PATH = "src/data/momentum/upbit/upbit_historical_data.json"
TICKERS_FILE_PATH = "src/data/momentum/upbit/upbit_tickers.json"

# ✅ 업비트 마켓 리스트 (재시도 적용)
@retry_on_failure(max_retries=4)
def get_krw_markets():
    url = f"{UPBIT_API_BASE}/market/all"
    res = requests.get(url)
    res.raise_for_status()
    return [m for m in res.json() if m["market"].startswith("KRW-")]

# ✅ 어제 종가 가져오기 (재시도 적용)
@retry_on_failure(max_retries=4)
def fetch_yesterday_candle(market):
    kst_now = datetime.utcnow() + timedelta(hours=9)
    kst_yesterday_end = (kst_now - timedelta(days=1)).strftime("%Y-%m-%dT23:59:59")
    url = f"{UPBIT_API_BASE}/candles/days"
    params = {"market": market, "count": 1, "to": kst_yesterday_end}
    res = requests.get(url, params=params)
    res.raise_for_status()
    time.sleep(0.15)  # API rate limit (증가)
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


# ========================================
# 단일 티커 처리 (병렬 실행용)
# ========================================
def process_single_ticker(args):
    """
    한 티커의 어제 데이터 가져오기 및 지표 계산

    Args:
        args: (market_info, ticker_map, yesterday_date)

    Returns:
        (mcv_id, updated_ticker_data) 또는 None
    """
    market_info, ticker_map, yesterday_date = args
    market = market_info["market"]
    ticker = market.replace("KRW-", "")
    mcv_id = f"{ticker}-KRW-UPBIT"

    # 어제 캔들 데이터 가져오기 (재시도 포함)
    candle = fetch_yesterday_candle(market)
    if not candle:
        return None

    candle_date = candle["candle_date_time_kst"][:10]

    # 새 레코드 생성
    new_record = {
        'date': candle_date,
        'open': candle["opening_price"],
        'high': candle["high_price"],
        'low': candle["low_price"],
        'close': candle["trade_price"],
        'volume': candle["candle_acc_trade_volume"],
        'rsi': None,
        'ema200_diff': None,
        'ema120_diff': None,
        'ema50_diff': None,
        'ema20_diff': None,
        'volume_ratio_90d': None,
        'volume_ratio_alltime': None
    }

    # 기존 티커 데이터 가져오기
    if mcv_id in ticker_map:
        ticker_data = ticker_map[mcv_id].copy()
    else:
        ticker_data = {
            'mcv_id': mcv_id,
            'ticker': ticker,
            'history': []
        }

    # Upsert: 같은 날짜 덮어쓰기 또는 추가
    ticker_data['history'] = upsert_history(ticker_data['history'], new_record)

    # 지표 계산 (최근 250일 데이터 사용)
    if len(ticker_data['history']) > 0:
        # 날짜 정렬
        ticker_data['history'].sort(key=lambda x: x['date'])

        # 지표 계산
        indicators = calculate_and_update_indicators(ticker_data['history'])

        # 최신 레코드 업데이트
        ticker_data['history'][-1].update(indicators)

    return (mcv_id, ticker_data)


# ✅ 메인 실행
def main():
    print("🚀 업비트 JSON 업데이트 시작 (upsert + 병렬 + 재시도)")

    # 1. 기존 JSON 로드
    json_data = load_json_data()
    ticker_map = {ticker['mcv_id']: ticker for ticker in json_data['data']}

    print(f"📊 기존 데이터: {len(ticker_map)}개 티커")

    # 2. 업비트 마켓 리스트 가져오기
    markets = get_krw_markets()
    yesterday_date = (datetime.utcnow() + timedelta(hours=9) - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📅 업데이트 날짜: {yesterday_date}")
    print(f"📋 총 {len(markets)}개 마켓 병렬 처리 중 (max_workers=3)...")

    # 3. 병렬 처리 (ThreadPoolExecutor, 재시도 포함)
    process_args = [(m, ticker_map, yesterday_date) for m in markets]
    results = parallel_process(
        func=process_single_ticker,
        items=process_args,
        max_workers=3,  # Rate limit 회피
        desc="업비트 티커 업데이트"
    )

    # 4. 결과 병합
    updated_count = 0
    new_tickers = []

    for mcv_id, ticker_data in results:
        is_new = mcv_id not in ticker_map
        ticker_map[mcv_id] = ticker_data
        updated_count += 1

        if is_new:
            new_tickers.append({'mcv_id': mcv_id, 'ticker': ticker_data['ticker']})

    # 3. JSON 저장
    json_data['data'] = list(ticker_map.values())
    json_data['total_tickers'] = len(ticker_map)
    json_data['total_records'] = sum(len(t['history']) for t in ticker_map.values())
    json_data['generated_at'] = datetime.now().isoformat()

    # ✅ cutoff_date를 실제 데이터의 최신 날짜로 업데이트
    all_dates = []
    for ticker_data in json_data['data']:
        for history_entry in ticker_data['history']:
            all_dates.append(history_entry['date'])
    if all_dates:
        json_data['cutoff_date'] = max(all_dates)
        print(f"📅 cutoff_date 업데이트: {json_data['cutoff_date']}")

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
