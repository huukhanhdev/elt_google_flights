"""
tests/test_client.py

Integration tests — gọi SerpApi thật, không mock.
Yêu cầu: SERPAPI_KEY có trong .env hoặc biến môi trường.

Chạy:
    python -m pytest tests/test_client.py -v -s
"""

import os
from datetime import date, timedelta

import pytest
from dotenv import load_dotenv

load_dotenv()

from extraction.client import SerpApiClient
from extraction.config import LOOKAHEAD_DAYS, ROUTES

# Bỏ qua toàn bộ test nếu chưa set SERPAPI_KEY
pytestmark = pytest.mark.skipif(
    not os.getenv("SERPAPI_KEY"),
    reason="SERPAPI_KEY chưa được set trong .env",
)


@pytest.fixture(scope="module")
def client():
    """Khởi tạo SerpApiClient một lần cho cả module."""
    return SerpApiClient()


@pytest.fixture(scope="module")
def run_date():
    return date.today()


# ─────────────────────────────────────────────────────────────
# Test 1: search_id tất định (không cần API)
# ─────────────────────────────────────────────────────────────

def test_search_id_deterministic():
    """Cùng input luôn cho cùng search_id."""
    c = SerpApiClient()
    sid1 = c._make_search_id("SGN", "HAN", date(2025, 4, 1), date(2025, 3, 1))
    sid2 = c._make_search_id("SGN", "HAN", date(2025, 4, 1), date(2025, 3, 1))
    assert sid1 == sid2
    assert len(sid1) == 32  # MD5 hex


def test_search_id_unique_for_different_routes():
    c = SerpApiClient()
    sid1 = c._make_search_id("SGN", "HAN", date(2025, 4, 1), date(2025, 3, 1))
    sid2 = c._make_search_id("HAN", "DAD", date(2025, 4, 1), date(2025, 3, 1))
    assert sid1 != sid2


# ─────────────────────────────────────────────────────────────
# Test 2: Gọi API thật — 1 route duy nhất
# ─────────────────────────────────────────────────────────────

def test_fetch_single_route_real_api(client, run_date):
    """
    Gọi SerpApi thật cho route SGN→HAN, departure sau 7 ngày.
    In kết quả ra để xem cấu trúc JSON trả về.
    """
    origin, destination = "SGN", "HAN"
    departure_date = run_date + timedelta(days=7)

    print(f"\n[TEST] Fetching {origin} → {destination} on {departure_date}")

    record = client._fetch_single_route(origin, destination, departure_date, run_date)

    # Phải trả về dict, không phải None
    assert record is not None, "API trả về None — kiểm tra lại SERPAPI_KEY hoặc quota"

    # Kiểm tra cấu trúc
    assert record["origin"] == origin
    assert record["destination"] == destination
    assert record["departure_date"] == departure_date
    assert record["run_date"] == run_date
    assert isinstance(record["search_id"], str) and len(record["search_id"]) == 32

    payload = record["raw_payload"]
    assert isinstance(payload, dict), "raw_payload phải là dict"

    # In tóm tắt kết quả
    best = payload.get("best_flights", [])
    print(f"  ✓ Số itinerary tìm được: {len(best)}")
    if best:
        first = best[0]
        price = first.get("price")
        airline = first.get("flights", [{}])[0].get("airline", "N/A")
        print(f"  ✓ Giá rẻ nhất: ${price} — Hãng: {airline}")

    print(f"  ✓ search_id: {record['search_id']}")
    print(f"  ✓ Top-level keys trong payload: {list(payload.keys())}")


# ─────────────────────────────────────────────────────────────
# Test 3: Gọi API thật — tất cả routes (fetch_all_routes)
# ─────────────────────────────────────────────────────────────

def test_fetch_all_routes_real_api(client, run_date):
    """
    Gọi toàn bộ ROUTES × LOOKAHEAD_DAYS từ config.
    In số record trả về và sample giá đầu tiên.
    Cảnh báo: tốn nhiều API quota hơn.
    """
    print(f"\n[TEST] fetch_all_routes cho run_date={run_date}")
    print(f"  Tổng số lời gọi API sẽ thực hiện: {len(ROUTES)} routes × {len(LOOKAHEAD_DAYS)} ngày = {len(ROUTES) * len(LOOKAHEAD_DAYS)}")

    records = client.fetch_all_routes(run_date=run_date)

    assert isinstance(records, list)
    assert len(records) > 0, "Không fetch được record nào — kiểm tra quota SerpApi"

    print(f"  ✓ Tổng records trả về: {len(records)}")
    for r in records[:3]:  # In 3 record đầu
        best = r["raw_payload"].get("best_flights", [])
        price = best[0].get("price") if best else "N/A"
        print(f"    {r['origin']}→{r['destination']} dep={r['departure_date']} giá=${price}")
