#!/usr/bin/env python3
"""
공통 유틸리티 함수 (병렬처리, 재시도, 지표계산)
모든 데이터 업데이트 스크립트에서 공유
"""

import time
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Any, List, Dict, Optional


# ========================================
# 재시도 로직 (4회, 지수 백오프)
# ========================================
def retry_on_failure(max_retries: int = 4, base_delay: float = 1.0):
    """
    API 호출 실패 시 재시도 데코레이터
    - 최대 4회 재시도
    - 지수 백오프: 1초 → 2초 → 4초 → 8초
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise  # 마지막 시도 실패 시 예외 발생

                    delay = base_delay * (2 ** attempt)
                    print(f"⚠️  {func.__name__} 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                    print(f"   {delay}초 후 재시도...")
                    time.sleep(delay)

            return None
        return wrapper
    return decorator


# ========================================
# 병렬 처리
# ========================================
def parallel_process(
    func: Callable,
    items: List[Any],
    max_workers: int = 10,
    desc: str = "처리 중"
) -> List[Any]:
    """
    병렬 처리 헬퍼 함수

    Args:
        func: 각 항목에 적용할 함수 (재시도 데코레이터 적용 권장)
        items: 처리할 항목 리스트
        max_workers: 동시 실행 쓰레드 수
        desc: 진행 상황 설명

    Returns:
        성공한 결과 리스트 (실패는 None 반환)
    """
    results = []
    total = len(items)
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(func, item): item for item in items}

        for future in as_completed(future_to_item):
            completed += 1

            # 진행률 출력 (10% 단위)
            if completed % max(1, total // 10) == 0 or completed == total:
                progress = (completed / total) * 100
                print(f"   {desc}: {completed}/{total} ({progress:.0f}%)")

            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                item = future_to_item[future]
                print(f"❌ {item} 처리 실패: {e}")

    return results


# ========================================
# RSI 계산 (Wilder 방식)
# ========================================
def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    """RSI 계산 (14일 기준)"""
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


# ========================================
# EMA 계산
# ========================================
def calculate_ema(prices: List[float], period: int) -> Optional[float]:
    """EMA 계산"""
    if len(prices) < 1:
        return None

    k = 2 / (period + 1)
    ema = prices[0]

    for price in prices[1:]:
        ema = price * k + ema * (1 - k)

    return round(ema, 8)


# ========================================
# Upsert 헬퍼 (같은 날짜 덮어쓰기)
# ========================================
def upsert_history(
    existing_history: List[Dict],
    new_record: Dict,
    date_key: str = 'date'
) -> List[Dict]:
    """
    히스토리에 새 레코드 추가 (같은 날짜 있으면 덮어쓰기)

    Args:
        existing_history: 기존 히스토리 리스트
        new_record: 추가할 새 레코드 (date 포함)
        date_key: 날짜 키 이름 (기본 'date')

    Returns:
        업데이트된 히스토리 리스트
    """
    new_date = new_record[date_key]

    # 같은 날짜 찾기
    for i, record in enumerate(existing_history):
        if record[date_key] == new_date:
            # 덮어쓰기
            existing_history[i] = new_record
            return existing_history

    # 새로운 날짜면 추가
    existing_history.append(new_record)
    return existing_history


# ========================================
# 지표 계산 및 업데이트
# ========================================
def calculate_and_update_indicators(
    history: List[Dict],
    close_key: str = 'close',
    volume_key: str = 'volume'
) -> Dict:
    """
    최근 250일 데이터로 RSI, EMA, 거래량 비율 계산

    Args:
        history: 전체 히스토리 (날짜 오름차순 정렬 필요)
        close_key: 종가 키 이름
        volume_key: 거래량 키 이름

    Returns:
        계산된 지표 딕셔너리
    """
    # 최근 250일 데이터
    recent_data = history[-250:]
    closes = [h[close_key] for h in recent_data]
    volumes = [h[volume_key] for h in recent_data if h.get(volume_key)]

    # RSI 계산
    rsi = calculate_rsi(closes)

    # EMA 계산
    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    ema120 = calculate_ema(closes, 120)
    ema200 = calculate_ema(closes, 200)

    current_price = closes[-1]

    # EMA 차이율
    ema20_diff = round(((current_price - ema20) / ema20) * 100, 2) if ema20 else None
    ema50_diff = round(((current_price - ema50) / ema50) * 100, 2) if ema50 else None
    ema120_diff = round(((current_price - ema120) / ema120) * 100, 2) if ema120 else None
    ema200_diff = round(((current_price - ema200) / ema200) * 100, 2) if ema200 else None

    # 거래량 비율
    vol_ratio_90d = None
    vol_ratio_alltime = None

    if volumes:
        current_volume = volumes[-1]

        # 90일 최대 거래량
        vol_max_90d = max(volumes[-90:]) if len(volumes) >= 90 else max(volumes)
        vol_ratio_90d = round(current_volume / vol_max_90d, 3) if vol_max_90d else None

        # 전체 기간 최대 거래량
        vol_max_alltime = max(volumes)
        vol_ratio_alltime = round(current_volume / vol_max_alltime, 3) if vol_max_alltime else None

    return {
        'rsi': rsi,
        'ema20_diff': ema20_diff,
        'ema50_diff': ema50_diff,
        'ema120_diff': ema120_diff,
        'ema200_diff': ema200_diff,
        'volume_ratio_90d': vol_ratio_90d,
        'volume_ratio_alltime': vol_ratio_alltime
    }
