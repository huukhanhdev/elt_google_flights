"""
extraction/client.py

SerpApiClient — fetches Google Flights data with retry logic.
"""

import hashlib
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from extraction.config import (
    LOOKAHEAD_DAYS,
    MAX_RETRIES,
    ROUTES,
    SERPAPI_BASE_URL,
    SERPAPI_ENGINE,
    WAIT_MAX_SECONDS,
    WAIT_MIN_SECONDS,
)

load_dotenv()
logger = logging.getLogger(__name__)


class SerpApiClient:
    """
    Fetches Google Flights search results from SerpApi.

    Usage:
        client = SerpApiClient()
        results = client.fetch_all_routes(run_date=date.today())
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("SERPAPI_KEY")
        if not self.api_key:
            raise ValueError("SERPAPI_KEY is not set. Check your .env file.")
        self.session = requests.Session()

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────

    def fetch_all_routes(self, run_date: date) -> List[Dict[str, Any]]:
        """
        Iterate over all configured routes × lookahead days.
        Returns a list of raw result dicts ready for the loader.
        """
        results = []
        for origin, destination in ROUTES:
            for days_ahead in LOOKAHEAD_DAYS:
                departure_date = run_date + timedelta(days=days_ahead)
                record = self._fetch_single_route(
                    origin=origin,
                    destination=destination,
                    departure_date=departure_date,
                    run_date=run_date,
                )
                if record:
                    results.append(record)
        logger.info("Fetched %d route-date combinations for run_date=%s", len(results), run_date)
        return results

    # ──────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────

    def _fetch_single_route(
        self,
        origin: str,
        destination: str,
        departure_date: date,
        run_date: date,
    ) -> Optional[Dict[str, Any]]:
        """Fetch one (origin, destination, departure_date) combination."""
        search_id = self._make_search_id(origin, destination, departure_date, run_date)
        try:
            payload = self._call_api(origin, destination, departure_date)
            return {
                "search_id": search_id,
                "origin": origin,
                "destination": destination,
                "departure_date": departure_date,
                "run_date": run_date,
                "raw_payload": payload,
            }
        except Exception as exc:
            logger.error(
                "Failed to fetch %s→%s on %s: %s",
                origin,
                destination,
                departure_date,
                exc,
            )
            return None

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=WAIT_MIN_SECONDS, max=WAIT_MAX_SECONDS),
        retry=retry_if_exception_type(requests.exceptions.ConnectionError)
        | retry_if_exception_type(requests.exceptions.Timeout)
        | retry_if_exception_type(requests.exceptions.ChunkedEncodingError),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _call_api(self, origin: str, destination: str, departure_date: date) -> Dict[str, Any]:
        """
        Call SerpApi Google Flights endpoint with retry.
        Only retries on transient network errors (timeout, connection dropped).
        Does NOT retry on 400/429 — those are permanent / quota errors.
        """
        params = {
            "engine": SERPAPI_ENGINE,
            "departure_id": origin,
            "arrival_id": destination,
            "outbound_date": departure_date.strftime("%Y-%m-%d"),
            "type": "2",       # 1 = round trip (requires return_date), 2 = one-way
            "currency": "USD",
            "hl": "en",
            "api_key": self.api_key,
        }
        logger.debug("GET %s params=%s", SERPAPI_BASE_URL, {k: v for k, v in params.items() if k != "api_key"})
        response = self.session.get(SERPAPI_BASE_URL, params=params, timeout=30)

        if response.status_code == 429:
            logger.warning("Rate limited by SerpApi (429). Will retry.")
            raise requests.exceptions.ConnectionError("429 Too Many Requests — treated as transient")

        if response.status_code == 400:
            try:
                detail = response.json().get("error", response.text)
            except Exception:
                detail = response.text
            logger.error("SerpApi 400 Bad Request for %s→%s on %s: %s", origin, destination, departure_date, detail)
            raise ValueError(f"SerpApi 400: {detail}")  # ValueError → không retry

        response.raise_for_status()

        data = response.json()
        if "error" in data:
            raise ValueError(f"SerpApi error: {data['error']}")
        return data

    @staticmethod
    def _make_search_id(origin: str, destination: str, departure_date: date, run_date: date) -> str:
        """
        Deterministic MD5 key used as idempotency key in the DB.
        Same (route, date, run_date) will always produce the same search_id.
        """
        raw = f"{origin}{destination}{departure_date}{run_date}"
        return hashlib.md5(raw.encode()).hexdigest()
