-- Populate dim_aircraft with SCD Type 2 awareness
-- New aircraft registrations are inserted; existing ones are left unchanged

INSERT OR IGNORE INTO dim_aircraft (
    aircraft_key,
    aircraft_registration,
    aircraft_type,
    airline_code,
    seat_capacity,
    effective_from,
    effective_to,
    is_current
)
SELECT
    ROW_NUMBER() OVER (ORDER BY sf.aircraft_registration) +
        COALESCE((SELECT MAX(aircraft_key) FROM dim_aircraft), 0) AS aircraft_key,
    sf.aircraft_registration,
    sf.aircraft_type,
    sf.airline_code,
    CASE sf.aircraft_type
        WHEN 'A320' THEN 180
        WHEN 'A321' THEN 220
        WHEN 'A350' THEN 305
        WHEN 'B787' THEN 274
        ELSE 180
    END AS seat_capacity,
    CURRENT_DATE AS effective_from,
    DATE '9999-12-31' AS effective_to,
    TRUE AS is_current
FROM (
    SELECT DISTINCT aircraft_registration, aircraft_type, airline_code
    FROM silver_flights
) sf
WHERE sf.aircraft_registration NOT IN (
    SELECT aircraft_registration FROM dim_aircraft WHERE is_current = TRUE
);
