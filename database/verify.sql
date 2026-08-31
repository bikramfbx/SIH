-- 1. PostGIS is installed
SELECT PostGIS_Version() AS postgis_version;

-- 2. hotspots table exists with correct columns
SELECT column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_name = 'hotspots'
ORDER BY ordinal_position;

-- 3. location column is geography(Point, 4326)
SELECT f_geometry_column, srid, type
FROM geometry_columns
WHERE f_table_name = 'hotspots';

-- 4. PostGIS spatial function works
SELECT ST_Distance(
    ST_SetSRID(ST_MakePoint(77.1025, 28.7041), 4326)::geography,
    ST_SetSRID(ST_MakePoint(77.2090, 28.6139), 4326)::geography
) AS delhi_distance_meters;

-- 5. Constraints exist
SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'hotspots'::regclass;
