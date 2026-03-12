-- Populate dim_routes from silver flights
-- Each unique origin-destination-airline combination gets a route entry

INSERT OR IGNORE INTO dim_routes (
    route_key,
    origin,
    destination,
    airline_code,
    route_code,
    distance_km,
    domestic
)
SELECT
    ROW_NUMBER() OVER (ORDER BY sf.origin, sf.destination, sf.airline_code) +
        COALESCE((SELECT MAX(route_key) FROM dim_routes), 0) AS route_key,
    sf.origin,
    sf.destination,
    sf.airline_code,
    sf.origin || '-' || sf.destination AS route_code,
    -- Estimated distance based on known routes
    CASE
        WHEN sf.origin = 'SGN' AND sf.destination = 'HAN' THEN 1180
        WHEN sf.origin = 'HAN' AND sf.destination = 'SGN' THEN 1180
        WHEN sf.origin = 'SGN' AND sf.destination = 'DAD' THEN 610
        WHEN sf.origin = 'DAD' AND sf.destination = 'SGN' THEN 610
        WHEN sf.origin = 'HAN' AND sf.destination = 'DAD' THEN 620
        WHEN sf.origin = 'DAD' AND sf.destination = 'HAN' THEN 620
        WHEN sf.origin = 'SGN' AND sf.destination = 'CXR' THEN 320
        WHEN sf.origin = 'CXR' AND sf.destination = 'SGN' THEN 320
        WHEN sf.origin = 'SGN' AND sf.destination = 'PQC' THEN 310
        WHEN sf.origin = 'PQC' AND sf.destination = 'SGN' THEN 310
        ELSE 500
    END AS distance_km,
    TRUE AS domestic
FROM (
    SELECT DISTINCT origin, destination, airline_code
    FROM silver_flights
    UNION
    SELECT DISTINCT origin, destination, airline_code
    FROM silver_reservations
) sf
WHERE NOT EXISTS (
    SELECT 1 FROM dim_routes dr
    WHERE dr.origin = sf.origin
      AND dr.destination = sf.destination
      AND dr.airline_code = sf.airline_code
);
