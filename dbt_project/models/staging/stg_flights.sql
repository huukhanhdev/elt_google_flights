-- models/staging/stg_flights.sql
--
-- Explodes raw JSONB payload into one row per flight offer.
-- Deduplicates within a (search_id, airline, price_usd) window.
-- Source: raw.flight_searches

WITH raw_source AS (

    SELECT
        search_id,
        origin,
        destination,
        departure_date,
        run_date,
        raw_payload,
        pulled_at
    FROM {{ source('raw', 'flight_searches') }}

),

-- Explode best_flights array: each element is one itinerary
exploded AS (

    SELECT
        r.search_id,
        r.origin,
        r.destination,
        r.departure_date,
        r.run_date,
        r.pulled_at,

        -- Top-level itinerary fields
        (flight->>'price')::NUMERIC                          AS price_usd,
        flight->>'type'                                      AS flight_type,

        -- First leg details (stored in flights[0] nested array)
        (flight->'flights'->0->>'airline')                    AS airline,
        (flight->'flights'->0->>'flight_number')              AS flight_number,
        (flight->'flights'->0->'departure_airport'->>'time')  AS departure_time,
        (flight->'flights'->0->'arrival_airport'->>'time')    AS arrival_time,
        (flight->'flights'->0->>'duration')::INT              AS duration_minutes,
        (flight->'flights'->0->>'travel_class')               AS travel_class,

        -- Layover count = number of legs - 1
        COALESCE(jsonb_array_length(flight->'flights') - 1, 0) AS stops,

        -- Raw booking token for traceability
        flight->>'booking_token'                             AS booking_token

    FROM raw_source r,
         LATERAL jsonb_array_elements(
             COALESCE(r.raw_payload->'best_flights', '[]'::jsonb)
         ) AS flight

),

-- Deduplication: keep first occurrence of each (search_id, airline, price_usd)
deduped AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY search_id, airline, price_usd
            ORDER BY pulled_at
        ) AS row_num

    FROM exploded

)

SELECT
    search_id,
    origin,
    destination,
    departure_date,
    run_date,
    airline,
    flight_number,
    departure_time,
    arrival_time,
    duration_minutes,
    travel_class,
    stops,
    price_usd,
    flight_type,
    booking_token,
    NOW() AS _loaded_at

FROM deduped
WHERE row_num = 1
  AND price_usd IS NOT NULL
  AND price_usd > 0
