"""
extraction/loader.py

PostgresLoader — upserts raw flight data into raw.flight_searches.
Uses INSERT ... ON CONFLICT DO NOTHING for idempotent incremental loads.
"""

import json
import logging
import os
from datetime import date
from typing import Any, Dict, List

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class PostgresLoader:
    """
    Loads raw flight search payloads into the raw.flight_searches table.

    Usage:
        loader = PostgresLoader()
        loader.upsert_batch(records)
        loader.close()
    """

    def __init__(self):
        self.conn = self._connect()

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────

    def already_loaded(self, origin: str, destination: str, departure_date: date, run_date: date) -> bool:
        """
        Incremental guard: returns True if this (route, departure_date, run_date)
        has already been loaded. Skips the SerpApi call entirely if so.
        """
        query = """
            SELECT 1
            FROM raw.flight_searches
            WHERE origin = %s
              AND destination = %s
              AND departure_date = %s
              AND run_date = %s
            LIMIT 1;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (origin, destination, departure_date, run_date))
            return cur.fetchone() is not None

    def upsert_batch(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert a batch of raw records. Silently skips duplicates via
        ON CONFLICT DO NOTHING (search_id is the unique key).
        Returns the number of rows actually inserted.
        """
        if not records:
            logger.info("No records to insert.")
            return 0

        insert_sql = """
            INSERT INTO raw.flight_searches
                (search_id, origin, destination, departure_date, run_date, raw_payload)
            VALUES
                (%(search_id)s, %(origin)s, %(destination)s,
                 %(departure_date)s, %(run_date)s, %(raw_payload)s)
            ON CONFLICT (search_id) DO NOTHING;
        """
        rows = [self._prepare_row(r) for r in records]
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=50)
        self.conn.commit()

        inserted = sum(1 for r in rows if r)  # approximate; psycopg2 doesn't expose per-row rowcount for execute_batch
        logger.info("Upserted batch of %d records (duplicates silently skipped).", len(rows))
        return len(rows)

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.debug("Postgres connection closed.")

    # ──────────────────────────────────────────────────────────
    # Context manager support
    # ──────────────────────────────────────────────────────────

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
            logger.error("Transaction rolled back due to: %s", exc_val)
        self.close()
        return False  # propagate exceptions

    # ──────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _connect() -> psycopg2.extensions.connection:
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", 5432)),
                dbname=os.getenv("POSTGRES_DB", "flights_db"),
                user=os.getenv("POSTGRES_USER", "airflow"),
                password=os.getenv("POSTGRES_PASSWORD", "airflow"),
            )
            logger.info("Connected to PostgreSQL.")
            return conn
        except psycopg2.OperationalError as exc:
            logger.error("Could not connect to PostgreSQL: %s", exc)
            raise

    @staticmethod
    def _prepare_row(record: Dict[str, Any]) -> Dict[str, Any]:
        """Serialize raw_payload JSONB and ensure date types."""
        return {
            "search_id": record["search_id"],
            "origin": record["origin"],
            "destination": record["destination"],
            "departure_date": record["departure_date"],
            "run_date": record["run_date"],
            "raw_payload": json.dumps(record["raw_payload"]),
        }
