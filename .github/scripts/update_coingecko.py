import requests
import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv

# ✅ .env 로드
load_dotenv()

# ✅ Supabase 설정
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
TABLE_NAME = "crypto_coingecko"
SAVE_PATH = "src/data/tickers/crypto/coingecko_with_mcv_id.json"

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("❌ 환경 변수 누락: SUPABASE_URL 또는 SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

# ✅ CoinGecko API 설정
COINGECKO_API = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 250,
    "page": 1,
    "sparkline": False
}

def fetch_all_coins():
    all_coins = []
    for page in range(1, 5):  # 250 * 4 = 상위 1000개
        print(f"📦 Fetching page {page}...")
        PARAMS["page"] = page
        
        # Rate limit 대응: 재시도 로직 추가
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(COINGECKO_API, params=PARAMS)
                if response.status_code == 429:  # Too Many Requests
                    wait_time = (attempt + 1) * 10  # 10, 20, 30초 대기
                    print(f"⚠️ Rate limit hit. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                all_coins.extend(response.json())
                break
            except requests.exceptions.HTTPError as e:
                if attempt == max_retries - 1:
                    raise e
                print(f"❌ Request failed (attempt {attempt + 1}): {e}")
                time.sleep(5)  # 5초 대기 후 재시도
        
        # 페이지 간 대기 시간 추가 (rate limit 예방)
        if page < 4:
            time.sleep(2)
    
    return all_coins

def normalize_coin_data(coins):
    result = []
    seen_ids = set()
    for coin in coins:
        ticker = coin["symbol"].upper()
        name = coin["name"]
        coingecko_id = coin["id"]
        marketcap = coin.get("market_cap")
        volume = coin.get("total_volume")
        common_ticker = f"{ticker}-USD"
        mcv_id = f"{common_ticker}-COINGECKO"

        if mcv_id in seen_ids:
            continue
        seen_ids.add(mcv_id)

        result.append({
            "ticker": ticker,
            "name": name,
            "ko_name": None,
            "type": "CRYPTO",
            "exchange": "COINGECKO",
            "currency": "USD",
            "marketcap": marketcap,
            "volume": volume,
            "coingecko_id": coingecko_id,
            "common_ticker": common_ticker,
            "mcv_id": mcv_id,
            "source": "COINGECKO",
        })
    return result

def save_to_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"💾 JSON saved to {path}")

def upload_to_supabase(data):
    print("📡 Supabase 업로드 시작...")
    url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?on_conflict=mcv_id"
    CHUNK_SIZE = 50
    total = len(data)
    for i in range(0, total, CHUNK_SIZE):
        chunk = data[i:i + CHUNK_SIZE]
        try:
            res = requests.post(url, headers=HEADERS, json=chunk)
            if res.status_code not in (200, 201):
                print(f"❌ 업서트 실패 (Chunk {i//CHUNK_SIZE+1}): {res.status_code} {res.text}")
            else:
                print(f"✅ 업서트 성공 (Chunk {i//CHUNK_SIZE+1}/{(total+CHUNK_SIZE-1)//CHUNK_SIZE})")
        except Exception as e:
            print(f"❌ 업서트 예외 발생: {e}")

if __name__ == "__main__":
    print("🚀 CoinGecko 데이터 수집 시작")
    coins = fetch_all_coins()
    normalized = normalize_coin_data(coins)
    save_to_json(normalized, SAVE_PATH)
    upload_to_supabase(normalized)
    print(f"🎉 전체 완료: {len(normalized)}개 저장됨 @ {datetime.utcnow().isoformat()} UTC")
