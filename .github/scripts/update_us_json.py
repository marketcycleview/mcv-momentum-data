#!/usr/bin/env python3
"""
미국 주식/ETF 일일 업데이트 (증분 업데이트)
- 기존 티커: 어제 데이터만 추가
- 신규 티커: 2022-01-01부터 전체 히스토리 다운로드

✨ 개선사항:
- upsert: 같은 날짜 데이터 덮어쓰기
- 병렬처리: ThreadPoolExecutor로 10개 동시 실행
- 재시도: API 실패 시 4회 재시도 (지수 백오프)
"""

import os
import json
import time
from datetime import datetime, timedelta
import yfinance as yf
from utils_common import retry_on_failure, parallel_process, upsert_history, calculate_and_update_indicators

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

# 신규 티커 최대 처리 개수 (rate limit 고려)
MAX_NEW_TICKERS = 5

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

    # 중복 제거 (mcv_id 기준)
    unique_tickers = {}
    for t in all_tickers:
        mcv_id = t.get('mcv_id')
        if mcv_id and mcv_id not in unique_tickers:
            unique_tickers[mcv_id] = t

    return list(unique_tickers.values())

# ✅ 기존 JSON 데이터 로드
def load_existing_data():
    if not os.path.exists(JSON_FILE_PATH):
        print("⚠️ 기존 JSON 파일 없음 - 재구축 스크립트를 먼저 실행하세요")
        return None

    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

# ✅ Yahoo Finance에서 최근 데이터 가져오기
@retry_on_failure(max_retries=4)
def fetch_yahoo_recent(ticker, start_date, end_date):
    """단일 날짜 또는 최근 며칠 데이터 가져오기 (재시도 포함)"""
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

    time.sleep(0.05)  # Rate limit
    return candles

# ✅ 전체 히스토리 가져오기 (신규 티커용)
@retry_on_failure(max_retries=4)
def fetch_yahoo_full_history(ticker, start_date="2022-01-01"):
    """2022-01-01부터 전체 히스토리 가져오기 (재시도 포함)"""
    end_date = datetime.now().strftime("%Y-%m-%d")
    return fetch_yahoo_recent(ticker, start_date, end_date)

# ✅ JSON 파일 저장
def save_json_data(data):
    os.makedirs(os.path.dirname(JSON_FILE_PATH), exist_ok=True)
    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ========================================
# 기존 티커 처리 (병렬 실행용)
# ========================================
def process_existing_ticker(args):
    """
    기존 티커의 어제 데이터 추가 (upsert)

    Args:
        args: (ticker_info, existing_ticker_data, yesterday)

    Returns:
        (mcv_id, updated_data) 또는 None
    """
    ticker_info, ticker_data, yesterday = args
    ticker = ticker_info['ticker']
    mcv_id = ticker_info['mcv_id']

    # 어제 데이터 가져오기 (재시도 포함)
    candles = fetch_yahoo_recent(ticker, yesterday, yesterday)

    if len(candles) == 0:
        return None  # 주말/휴일

    # Upsert: 같은 날짜 덮어쓰기
    for candle in candles:
        ticker_data['history'] = upsert_history(ticker_data['history'], candle)

    # 날짜 정렬
    ticker_data['history'].sort(key=lambda x: x['date'])

    # 지표 재계산
    if len(ticker_data['history']) > 0:
        indicators = calculate_and_update_indicators(ticker_data['history'])
        ticker_data['history'][-1].update(indicators)

    return (mcv_id, ticker_data)


# ========================================
# 신규 티커 처리 (병렬 실행용)
# ========================================
def process_new_ticker(ticker_info):
    """
    신규 티커의 전체 히스토리 다운로드

    Args:
        ticker_info: 티커 정보 딕셔너리

    Returns:
        ticker_data 또는 None
    """
    ticker = ticker_info['ticker']
    mcv_id = ticker_info['mcv_id']
    ko_name = ticker_info.get('ko_name')

    # 전체 히스토리 다운로드 (재시도 포함)
    candles = fetch_yahoo_full_history(ticker)

    if len(candles) == 0:
        return None

    # 지표 계산
    if len(candles) > 0:
        indicators = calculate_and_update_indicators(candles)
        candles[-1].update(indicators)

    return {
        'mcv_id': mcv_id,
        'ticker': ticker,
        'ko_name': ko_name,
        'history': candles
    }


# ✅ 메인 실행
def main():
    print("🔄 미국 주식/ETF 일일 업데이트 시작 (upsert + 병렬 + 재시도)...")

    # 어제 날짜 계산 (미국 시장 기준)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 업데이트 날짜: {yesterday}\n")

    # 1. 티커 소스 로드
    local_tickers = load_local_tickers()
    print(f"📋 로컬 티커: {len(local_tickers)}개")

    # TODO: Watchlist 티커 가져오기 (DB 연동)
    # watchlist_tickers = get_watchlist_tickers()
    # all_tickers = merge_unique(local_tickers, watchlist_tickers)
    all_tickers = local_tickers  # 현재는 로컬만

    # 2. 기존 JSON 데이터 로드
    existing_data = load_existing_data()
    if not existing_data:
        print("❌ 기존 데이터 없음 - rebuild_us_history.py를 먼저 실행하세요")
        return

    # 기존 티커 맵 생성
    existing_map = {item['mcv_id']: item for item in existing_data['data']}
    existing_mcv_ids = set(existing_map.keys())

    # 3. 신규 티커 vs 기존 티커 분리
    new_tickers = [t for t in all_tickers if t['mcv_id'] not in existing_mcv_ids]
    existing_tickers = [t for t in all_tickers if t['mcv_id'] in existing_mcv_ids]

    print(f"🆕 신규 티커: {len(new_tickers)}개")
    print(f"🔄 기존 티커: {len(existing_tickers)}개\n")

    # 4. 기존 티커 업데이트 (병렬 처리 + upsert)
    print(f"📊 기존 티커 업데이트 중 (max_workers=10)...")
    process_args = [(t, existing_map[t['mcv_id']].copy(), yesterday) for t in existing_tickers]
    results = parallel_process(
        func=process_existing_ticker,
        items=process_args,
        max_workers=10,
        desc="기존 티커 업데이트"
    )

    # 결과 병합
    for mcv_id, updated_data in results:
        existing_map[mcv_id] = updated_data

    print(f"✅ 기존 티커 업데이트 완료: {len(results)}개\n")

    # 5. 신규 티커 처리 (병렬 처리)
    if len(new_tickers) > 0:
        # 최대 5개까지만 처리 (나머지는 다음날)
        if len(new_tickers) > MAX_NEW_TICKERS:
            print(f"⚠️ 신규 티커 {len(new_tickers)}개 감지 - 오늘은 {MAX_NEW_TICKERS}개만 처리")
            process_new = new_tickers[:MAX_NEW_TICKERS]
        else:
            process_new = new_tickers

        print(f"🆕 신규 티커 처리 중 (max_workers=10)...")
        new_results = parallel_process(
            func=process_new_ticker,
            items=process_new,
            max_workers=10,
            desc="신규 티커 다운로드"
        )

        # 결과 추가
        for ticker_data in new_results:
            existing_data['data'].append(ticker_data)

        print(f"✅ 신규 티커 처리 완료: {len(new_results)}개\n")

    # 6. 메타데이터 업데이트
    existing_data['generated_at'] = datetime.now().isoformat()
    existing_data['total_tickers'] = len(existing_data['data'])
    existing_data['total_records'] = sum(len(t['history']) for t in existing_data['data'])

    # 7. JSON 저장
    save_json_data(existing_data)

    print("✅ 일일 업데이트 완료!")
    print(f"   - 총 티커: {existing_data['total_tickers']}개")
    print(f"   - 총 레코드: {existing_data['total_records']}")
    print(f"   - 업데이트 시각: {existing_data['generated_at']}")

    if os.path.exists(JSON_FILE_PATH):
        file_size = os.path.getsize(JSON_FILE_PATH) / 1024 / 1024
        print(f"   - 파일 크기: {file_size:.2f} MB")

if __name__ == "__main__":
    main()
