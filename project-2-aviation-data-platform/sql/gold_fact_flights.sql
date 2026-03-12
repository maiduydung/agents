-- Populate fact_flights from silver_flights joined to dimensions
-- Incremental: only processes flights not yet in the fact table

INSERT OR IGNORE INTO fact_flights (
    flight_key,
    flight_id,
    flight_number,
    date_key,
    route_key,
    aircraft_key,
    scheduled_departure,
    actual_departure,
    status,
    delay_minutes,
    pax_count,
    load_factor,
    fuel_kg,
    _processed_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY sf.flight_id) +
        COALESCE((SELECT MAX(flight_key) FROM fact_flights), 0) AS flight_key,
    sf.flight_id,
    sf.flight_number,
    CAST(strftime(CAST(sf.scheduled_departure AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
    dr.route_key,
    da.aircraft_key,
    sf.scheduled_departure,
    sf.actual_departure,
    sf.status,
    sf.delay_minutes,
    sf.pax_count,
    CASE
        WHEN da.seat_capacity > 0 THEN ROUND(CAST(sf.pax_count AS DOUBLE) / da.seat_capacity, 4)
        ELSE 0.0
    END AS load_factor,
    sf.fuel_kg,
    CURRENT_TIMESTAMP AS _processed_at
FROM silver_flights sf
LEFT JOIN dim_routes dr
    ON sf.origin = dr.origin
    AND sf.destination = dr.destination
    AND sf.airline_code = dr.airline_code
LEFT JOIN dim_aircraft da
    ON sf.aircraft_registration = da.aircraft_registration
    AND da.is_current = TRUE
WHERE sf.flight_id NOT IN (SELECT flight_id FROM fact_flights);
