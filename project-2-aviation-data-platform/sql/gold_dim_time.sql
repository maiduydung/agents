-- Populate dim_time from silver flight dates
-- Idempotent: uses INSERT OR IGNORE to avoid duplicates

INSERT OR IGNORE INTO dim_time (
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend,
    is_holiday
)
SELECT DISTINCT
    CAST(strftime(d.flight_date, '%Y%m%d') AS INTEGER) AS date_key,
    d.flight_date AS full_date,
    EXTRACT(YEAR FROM d.flight_date) AS year,
    EXTRACT(QUARTER FROM d.flight_date) AS quarter,
    EXTRACT(MONTH FROM d.flight_date) AS month,
    strftime(d.flight_date, '%B') AS month_name,
    EXTRACT(WEEK FROM d.flight_date) AS week_of_year,
    EXTRACT(DAY FROM d.flight_date) AS day_of_month,
    EXTRACT(DOW FROM d.flight_date) AS day_of_week,
    strftime(d.flight_date, '%A') AS day_name,
    CASE WHEN EXTRACT(DOW FROM d.flight_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday
FROM (
    SELECT CAST(scheduled_departure AS DATE) AS flight_date FROM silver_flights
    UNION
    SELECT departure_date AS flight_date FROM silver_reservations
) d
WHERE CAST(strftime(d.flight_date, '%Y%m%d') AS INTEGER) NOT IN (
    SELECT date_key FROM dim_time
);
