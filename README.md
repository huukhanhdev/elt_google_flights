# ✈️ Flight Price Intelligence — ELT Pipeline

A production-like **end-to-end ELT pipeline** that ingests real Google Flights data via [SerpApi](https://serpapi.com/), stores raw JSON in PostgreSQL, transforms it through **dbt**, and orchestrates everything with **Apache Airflow** — fully containerised with Docker Compose.

> Built as a Data Engineering portfolio project (Intern level).

---

## 📐 Architecture

```
┌─────────────────────────────────────────────┐
│           Apache Airflow (daily @ 08:00 UTC) │
│                                              │
│  extract_and_load → dbt_staging →            │
│  dbt_mart         → dbt_test                 │
└──────────┬───────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────┐
│                 PostgreSQL                   │
│                                              │
│  raw.flight_searches    ← JSONB (SerpApi)    │
│  staging.stg_flights    ← exploded, typed    │
│  mart.cheapest_routes   ← RANK, AVG OVER     │
│  mart.price_trends      ← LAG, running MIN   │
└──────────────────────────────────────────────┘
```

**ELT pattern:** raw JSON is loaded first, transformations run entirely inside the database via dbt SQL.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python (`requests`, OOP, `tenacity` retry) |
| Storage | PostgreSQL 15 (JSONB raw layer) |
| Transformation | dbt (`staging` views → `mart` tables) |
| Orchestration | Apache Airflow 2.8.1 |
| Infrastructure | Docker Compose |
| Testing | `pytest` (integration tests, real API calls) |

---

## 📁 Project Structure

```
ETL_project/
├── docker-compose.yml
├── .env.example
├── requirements.txt
├── sql/
│   └── init.sql                  # Schema bootstrap (raw/staging/mart)
├── extraction/
│   ├── config.py                 # Routes, lookahead days
│   ├── client.py                 # SerpApiClient — OOP, retry, idempotency key
│   └── loader.py                 # PostgresLoader — upsert, incremental guard
├── dags/
│   └── flight_pipeline_dag.py    # Airflow DAG
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/stg_flights.sql
│       └── mart/
│           ├── cheapest_routes.sql
│           └── price_trends.sql
├── monitoring/
│   └── alert_hooks.py            # on_failure_callback + Slack webhook
└── tests/
    └── test_client.py            # Integration tests (real SerpApi calls)
```

---

## 🚀 Quick Start

### Prerequisites
- Docker + Docker Compose
- SerpApi account → [get free API key](https://serpapi.com/manage-api-key) (250 req/month free)

### 1. Configure environment
```bash
cp .env.example .env
# Fill in: SERPAPI_KEY, POSTGRES_USER, POSTGRES_PASSWORD
```

### 2. Initialise Airflow (first time only)
```bash
docker-compose run --rm airflow-init
```

### 3. Start the stack
```bash
docker-compose up -d
```

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8081 | admin / admin |
| PostgreSQL | localhost:5434 | see `.env` |

### 4. Trigger the pipeline
Airflow UI → **flight_price_pipeline** → ▶ **Trigger DAG**

---

## 🔍 Key Design Decisions

### Idempotent Deduplication
Each API call is assigned a **deterministic MD5 key** based on `(origin, destination, departure_date, run_date)`. The database enforces uniqueness; inserts silently skip duplicates:
```sql
INSERT INTO raw.flight_searches (...) VALUES (...)
ON CONFLICT (search_id) DO NOTHING;
```

### Incremental Loading
Before calling SerpApi, the pipeline checks whether a route-date combination was already loaded today — skipping the API call entirely if so. Saves API quota on retries.

### Window Functions in dbt
| Model | Functions Used |
|---|---|
| `stg_flights` | `ROW_NUMBER() OVER (...)` — deduplication |
| `cheapest_routes` | `RANK() OVER (...)`, `AVG() OVER (...)` — price ranking |
| `price_trends` | `LAG() OVER (...)`, running `MIN() OVER (...)` — day-over-day delta |

### Retry Logic
API calls use `tenacity` with exponential backoff (3 attempts, 2–10s wait). Airflow tasks also have `retries=2` with a 5-minute delay.

---

## ✅ Running Tests

```bash
pip install -r requirements.txt

# Single route — costs 1 API request
pytest tests/test_client.py::test_fetch_single_route_real_api -v -s

# All routes — costs 6 × 4 = 24 API requests
pytest tests/test_client.py -v -s
```

---

## 📊 Sample Queries

```sql
-- Cheapest flight per route today
SELECT origin, destination, airline, price_usd, stops
FROM mart.cheapest_routes
WHERE price_rank = 1
ORDER BY price_usd;

-- Day-over-day price movement
SELECT origin, destination, run_date,
       avg_price_usd, price_change_usd, price_change_pct, is_new_low
FROM mart.price_trends
WHERE price_change_usd IS NOT NULL
ORDER BY run_date DESC;
```

---

## 🗂️ Routes Monitored

| Origin | Destination |
|---|---|
| Ho Chi Minh City (SGN) | Hanoi (HAN) |
| Hanoi (HAN) | Da Nang (DAD) |
| Ho Chi Minh City (SGN) | Da Nang (DAD) |
| Ho Chi Minh City (SGN) | Bangkok (BKK) |
| Hanoi (HAN) | Bangkok (BKK) |
| Ho Chi Minh City (SGN) | Singapore (SIN) |

Prices are tracked for departures **1, 7, 14, and 30 days** ahead.

---

## 📄 License

MIT
