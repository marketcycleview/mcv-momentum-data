#!/usr/bin/env python3
"""
업비트 전체 히스토리 재구축 (2022-01-01부터)
업비트 API에서 직접 가져와서 JSON 생성

✨ 개선사항:
- 병렬처리: ThreadPoolExecutor로 10개 동시 실행
- 재시도: API 실패 시 4회 재시도 (지수 백오프)
"""

import os
import requests
import json
import time
from datetime import datetime, timedelta
from utils_common import retry_on_failure, parallel_process, calculate_and_update_indicators

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

# ✅ 단일 페이지 캔들 가져오기 (재시도 적용)
@retry_on_failure(max_retries=4)
def fetch_single_page_candles(market, to_date, count=200):
    """한 페이지(최대 200개) 캔들 가져오기 - 재시도 포함"""
    url = f"{UPBIT_API_BASE}/candles/days"
    params = {
        "market": market,
        "count": count,
        "to": f"{to_date}T23:59:59"
    }
    res = requests.get(url, params=params, timeout=10)
    res.raise_for_status()
    time.sleep(0.15)  # API rate limit (증가)
    return res.json()

# ✅ 특정 기간의 전체 캔들 데이터 가져오기 (페이징 처리)
def fetch_candles(market, start_date, end_date):
    """
    start_date부터 end_date까지의 일봉 데이터 수집
    업비트 API는 최대 200개씩만 반환하므로 페이징 필요
    """
    all_candles = []
    current_end = end_date
    max_iterations = 20  # 최대 20번 반복 (무한루프 방지)

    for iteration in range(max_iterations):
        # 재시도 포함된 API 호출
        candles = fetch_single_page_candles(market, current_end)

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


# ========================================
# 단일 마켓 처리 (병렬 실행용)
# ========================================
def process_single_market(args):
    """
    한 마켓의 전체 히스토리 가져오기 및 지표 계산

    Args:
        args: (market_info, start_date, end_date)

    Returns:
        (ticker_data, ticker_info) 또는 None
    """
    market_info, start_date, end_date = args
    market = market_info["market"]
    ticker = market.replace("KRW-", "")
    mcv_id = f"{ticker}-KRW-UPBIT"

    # 전체 기간 데이터 가져오기 (재시도 포함)
    candles = fetch_candles(market, start_date, end_date)

    if len(candles) == 0:
        return None

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

    # 지표 계산 (최근 250일 데이터 사용)
    if len(history) > 0:
        indicators = calculate_and_update_indicators(history)
        history[-1].update(indicators)

    ticker_data = {
        'mcv_id': mcv_id,
        'ticker': ticker,
        'history': history
    }

    ticker_info = {'mcv_id': mcv_id, 'ticker': ticker}

    return (ticker_data, ticker_info)


# ✅ 메인 실행
def main():
    print("🚀 업비트 전체 히스토리 재구축 시작 (병렬 + 재시도)")

    start_date = "2022-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"📅 수집 기간: {start_date} ~ {end_date}")

    # 1. 마켓 리스트 가져오기
    markets = get_krw_markets()
    print(f"📋 총 {len(markets)}개 마켓 병렬 처리 중 (max_workers=3)...")

    # 2. 병렬 처리 (ThreadPoolExecutor, 재시도 포함)
    process_args = [(m, start_date, end_date) for m in markets]
    results = parallel_process(
        func=process_single_market,
        items=process_args,
        max_workers=3,  # Rate limit 회피
        desc="업비트 티커 재구축"
    )

    # 3. 결과 수집
    all_data = []
    all_tickers = []

    for ticker_data, ticker_info in results:
        all_data.append(ticker_data)
        all_tickers.append(ticker_info)

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
