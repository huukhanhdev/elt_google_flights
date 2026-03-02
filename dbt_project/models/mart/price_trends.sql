-- models/mart/price_trends.sql
--
-- Tracks how the average daily price of each route evolves over pipeline run_dates.
-- Window functions: LAG() OVER to compute day-over-day price change.

WITH daily_avg AS (

    SELECT
        origin,
        destination,
        departure_date,
        run_date,
        ROUND(AVG(price_usd), 2)  AS avg_price_usd,
        MIN(price_usd)             AS min_price_usd,
        COUNT(*)                   AS offer_count
    FROM {{ ref('stg_flights') }}
    GROUP BY 1, 2, 3, 4

),

with_lag AS (

    SELECT
        *,

        -- Previous day's average price for the same route + departure_date
        LAG(avg_price_usd) OVER (
            PARTITION BY origin, destination, departure_date
            ORDER BY run_date
        ) AS prev_day_avg_price,

        -- Day-on-day delta: how much the price moved since yesterday
        avg_price_usd - LAG(avg_price_usd) OVER (
            PARTITION BY origin, destination, departure_date
            ORDER BY run_date
        ) AS price_change_usd,

        -- Cumulative min price seen so far for this route (running window)
        MIN(avg_price_usd) OVER (
            PARTITION BY origin, destination, departure_date
            ORDER BY run_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS all_time_min_price

    FROM daily_avg

)

SELECT
    origin,
    destination,
    departure_date,
    run_date,
    avg_price_usd,
    min_price_usd,
    offer_count,
    prev_day_avg_price,
    price_change_usd,

    -- Percentage price change vs previous day
    ROUND(
        price_change_usd / NULLIF(prev_day_avg_price, 0) * 100,
        2
    ) AS price_change_pct,

    all_time_min_price,

    -- Is today's price the lowest ever seen for this route? Useful for alerts.
    CASE
        WHEN avg_price_usd = all_time_min_price AND prev_day_avg_price IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END AS is_new_low

FROM with_lag
ORDER BY origin, destination, departure_date, run_date
