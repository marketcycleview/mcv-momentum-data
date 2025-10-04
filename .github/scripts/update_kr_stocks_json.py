#!/usr/bin/env python3
"""
한국 주식 일일 업데이트 (증분 업데이트)
- 기존 종목: 어제 데이터만 추가
- 신규 종목: 2022-01-01부터 전체 히스토리 다운로드
"""

import os
import json
import time
from datetime import datetime, timedelta
import yfinance as yf

# 경로 설정
JSON_FILE_PATH = "src/data/momentum/kr/kr_stocks_historical_data.json"
TICKERS_FILE_PATH = "src/data/momentum/kr/kr_stocks_tickers.json"

# 로컬 티커 파일 경로
TICKER_FILE = "src/data/tickers/kr/stocks/korea_stocks_with_mcv_id.json"

# 신규 종목 최대 처리 개수
MAX_NEW_TICKERS = 5

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
    if not os.path.exists(TICKER_FILE):
        print(f"⚠️ 티커 파일 없음: {TICKER_FILE}")
        return []

    with open(TICKER_FILE, 'r', encoding='utf-8') as f:
        tickers = json.load(f)

    # 중복 제거 및 category 추가
    unique_tickers = {}
    for t in tickers:
        mcv_id = t.get('mcv_id')
        if mcv_id and mcv_id not in unique_tickers:
            ticker = t.get('ticker', '')
            if '.KS' in ticker:
                t['category'] = 'kospi'
            elif '.KQ' in ticker:
                t['category'] = 'kosdaq'
            else:
                t['category'] = 'unknown'
            unique_tickers[mcv_id] = t

    return list(unique_tickers.values())

# ✅ 기존 JSON 데이터 로드
def load_existing_data():
    if not os.path.exists(JSON_FILE_PATH):
        print("⚠️ 기존 JSON 파일 없음 - rebuild 스크립트를 먼저 실행하세요")
        return None

    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

# ✅ Yahoo Finance에서 최근 데이터 가져오기
def fetch_yahoo_recent(ticker, start_date, end_date):
    """단일 날짜 또는 최근 며칠 데이터 가져오기"""
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
        print(f"❌ {ticker} 에러: {e}")
        return []

# ✅ 전체 히스토리 가져오기 (신규 종목용)
def fetch_yahoo_full_history(ticker, start_date="2022-01-01"):
    """2022-01-01부터 전체 히스토리 가져오기"""
    end_date = datetime.now().strftime("%Y-%m-%d")
    return fetch_yahoo_recent(ticker, start_date, end_date)

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
    print("🔄 한국 주식 일일 업데이트 시작...")

    # 어제 날짜 계산
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 업데이트 날짜: {yesterday}\n")

    # 1. 티커 소스 로드
    local_tickers = load_local_tickers()
    print(f"📋 로컬 티커: {len(local_tickers)}개")

    all_tickers = local_tickers

    # 2. 기존 JSON 데이터 로드
    existing_data = load_existing_data()
    if not existing_data:
        print("❌ 기존 데이터 없음 - rebuild_kr_stocks_history.py를 먼저 실행하세요")
        return

    # 기존 종목 맵 생성
    existing_map = {item['mcv_id']: item for item in existing_data['data']}
    existing_mcv_ids = set(existing_map.keys())

    # 3. 신규 종목 vs 기존 종목 분리
    new_tickers = [t for t in all_tickers if t['mcv_id'] not in existing_mcv_ids]
    existing_tickers = [t for t in all_tickers if t['mcv_id'] in existing_mcv_ids]

    print(f"🆕 신규 종목: {len(new_tickers)}개")
    print(f"🔄 기존 종목: {len(existing_tickers)}개\n")

    # 4. 기존 종목 업데이트 (어제 데이터만 추가)
    updated_count = 0
    print("📊 기존 종목 업데이트 중...")
    for t in existing_tickers:
        ticker = t['ticker']
        mcv_id = t['mcv_id']

        try:
            # 어제 데이터 가져오기
            candles = fetch_yahoo_recent(ticker, yesterday, yesterday)

            if len(candles) == 0:
                continue  # 주말/휴일이면 스킵

            # 기존 히스토리에 추가
            ticker_data = existing_map[mcv_id]

            # 중복 체크
            existing_dates = {h['date'] for h in ticker_data['history']}
            for candle in candles:
                if candle['date'] not in existing_dates:
                    ticker_data['history'].append(candle)

            # 지표 재계산
            calculate_and_update_indicators(ticker_data['history'])
            updated_count += 1

            if updated_count % 50 == 0:
                print(f"   진행: {updated_count}/{len(existing_tickers)}")

        except Exception as e:
            print(f"❌ {ticker} 업데이트 실패: {e}")
            continue

        time.sleep(0.05)

    print(f"✅ 기존 종목 업데이트 완료: {updated_count}개\n")

    # 5. 신규 종목 처리
    if len(new_tickers) > 0:
        print(f"🆕 신규 종목 처리 중...")

        if len(new_tickers) > MAX_NEW_TICKERS:
            print(f"⚠️ 신규 종목 {len(new_tickers)}개 감지 - 오늘은 {MAX_NEW_TICKERS}개만 처리")
            process_new = new_tickers[:MAX_NEW_TICKERS]
        else:
            process_new = new_tickers

        for t in process_new:
            ticker = t['ticker']
            mcv_id = t['mcv_id']
            ko_name = t.get('ko_name')
            category = t.get('category', 'unknown')

            print(f"   🆕 {ticker} - 2022-01-01부터 다운로드 중...", end=" ", flush=True)

            try:
                candles = fetch_yahoo_full_history(ticker)

                if len(candles) == 0:
                    print("❌ 데이터 없음")
                    continue

                print(f"✅ {len(candles)}개")

                # 지표 계산
                calculate_and_update_indicators(candles)

                # 데이터에 추가
                existing_data['data'].append({
                    'mcv_id': mcv_id,
                    'ticker': ticker,
                    'ko_name': ko_name,
                    'category': category,
                    'history': candles
                })

                time.sleep(0.1)

            except Exception as e:
                print(f"❌ {ticker} 다운로드 실패: {e}")
                continue

        print(f"✅ 신규 종목 처리 완료: {len(process_new)}개\n")

    # 6. 메타데이터 업데이트
    existing_data['generated_at'] = datetime.now().isoformat()
    existing_data['total_tickers'] = len(existing_data['data'])
    existing_data['total_records'] = sum(len(t['history']) for t in existing_data['data'])

    # 7. JSON 저장
    save_json_data(existing_data)

    print("✅ 일일 업데이트 완료!")
    print(f"   - 총 종목: {existing_data['total_tickers']}개")
    print(f"   - 총 레코드: {existing_data['total_records']}")
    print(f"   - 업데이트 시각: {existing_data['generated_at']}")

    if os.path.exists(JSON_FILE_PATH):
        file_size = os.path.getsize(JSON_FILE_PATH) / 1024 / 1024
        print(f"   - 파일 크기: {file_size:.2f} MB")

if __name__ == "__main__":
    main()
