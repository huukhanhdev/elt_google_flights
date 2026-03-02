-- models/mart/cheapest_routes.sql
--
-- For each route + departure_date: cheapest airline, price rank, and
-- how each price compares to the route's daily average.
-- Window functions: RANK(), AVG() OVER

WITH base AS (

    SELECT
        origin,
        destination,
        departure_date,
        run_date,
        airline,
        flight_number,
        duration_minutes,
        stops,
        price_usd,
        travel_class
    FROM {{ ref('stg_flights') }}

),

ranked AS (

    SELECT
        *,

        -- Rank flights from cheapest to most expensive within each route-day
        RANK() OVER (
            PARTITION BY origin, destination, departure_date, run_date
            ORDER BY price_usd ASC
        ) AS price_rank,

        -- Average price across all airlines for this route-day
        AVG(price_usd) OVER (
            PARTITION BY origin, destination, departure_date, run_date
        ) AS avg_price_route_day,

        -- Minimum price for this route-day
        MIN(price_usd) OVER (
            PARTITION BY origin, destination, departure_date, run_date
        ) AS min_price_route_day,

        -- Maximum price for this route-day
        MAX(price_usd) OVER (
            PARTITION BY origin, destination, departure_date, run_date
        ) AS max_price_route_day

    FROM base

)

SELECT
    origin,
    destination,
    departure_date,
    run_date,
    airline,
    flight_number,
    duration_minutes,
    stops,
    travel_class,
    price_usd,
    price_rank,
    ROUND(avg_price_route_day, 2)  AS avg_price_usd,
    ROUND(min_price_route_day, 2)  AS min_price_usd,
    ROUND(max_price_route_day, 2)  AS max_price_usd,

    -- How much cheaper/more expensive vs route average (negative = cheaper)
    ROUND(price_usd - avg_price_route_day, 2) AS diff_from_avg,

    -- Percentage below/above average
    ROUND(
        (price_usd - avg_price_route_day) / NULLIF(avg_price_route_day, 0) * 100,
        2
    ) AS pct_diff_from_avg

FROM ranked
