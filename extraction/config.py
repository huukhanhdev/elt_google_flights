"""
extraction/config.py
Route and search configuration for the flight pipeline.
"""

# City-pair routes to monitor
ROUTES = [
    ("SGN", "HAN"),  # Ho Chi Minh City → Hanoi
    ("HAN", "DAD"),  # Hanoi → Da Nang
    ("SGN", "DAD"),  # Ho Chi Minh City → Da Nang
    ("SGN", "BKK"),  # Ho Chi Minh City → Bangkok
    ("HAN", "BKK"),  # Hanoi → Bangkok
    ("SGN", "SIN"),  # Ho Chi Minh City → Singapore
]

# How many days ahead to search for flights
LOOKAHEAD_DAYS = [1, 7, 14, 30]

# SerpApi endpoint
SERPAPI_BASE_URL = "https://serpapi.com/search"
SERPAPI_ENGINE = "google_flights"

# Tenacity retry settings
MAX_RETRIES = 3
WAIT_MIN_SECONDS = 2
WAIT_MAX_SECONDS = 10
