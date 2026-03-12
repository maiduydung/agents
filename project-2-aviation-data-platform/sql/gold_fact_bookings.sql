-- Populate fact_bookings from silver_reservations joined to dimensions
-- Incremental: only processes bookings not yet in the fact table

INSERT OR IGNORE INTO fact_bookings (
    booking_key,
    reservation_id,
    flight_number,
    date_key,
    route_key,
    airline_code,
    fare_class,
    fare_amount,
    currency,
    booking_channel,
    status,
    _processed_at
)
SELECT
    ROW_NUMBER() OVER (ORDER BY sr.reservation_id) +
        COALESCE((SELECT MAX(booking_key) FROM fact_bookings), 0) AS booking_key,
    sr.reservation_id,
    sr.flight_number,
    CAST(strftime(sr.departure_date, '%Y%m%d') AS INTEGER) AS date_key,
    dr.route_key,
    sr.airline_code,
    sr.fare_class,
    sr.fare_amount,
    sr.currency,
    sr.booking_channel,
    sr.status,
    CURRENT_TIMESTAMP AS _processed_at
FROM silver_reservations sr
LEFT JOIN dim_routes dr
    ON sr.origin = dr.origin
    AND sr.destination = dr.destination
    AND sr.airline_code = dr.airline_code
WHERE sr.reservation_id NOT IN (SELECT reservation_id FROM fact_bookings);
