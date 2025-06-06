CREATE TABLE IF NOT EXISTS traffic_incidents (
    id SERIAL PRIMARY KEY,
    incident_timestamp TIMESTAMP NOT NULL,
    borough VARCHAR(50),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    street_name VARCHAR(100),
    injured_persons INTEGER,
    killed_persons INTEGER,
    vehicle_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    crash_instant TIMESTAMP,
    day_of_week TEXT,
    month TEXT,
    year INT
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    borough TEXT,
    zip_code TEXT,
    on_street_name TEXT,
    cross_street_name TEXT,
    off_street_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_id SERIAL PRIMARY KEY,
    vehicle_type TEXT,
    vehicle_position INT  -- 1 to 5
);

CREATE TABLE IF NOT EXISTS dim_contributing_factor (
    factor_id SERIAL PRIMARY KEY,
    factor_description TEXT,
    factor_position INT  -- 1 to 5
);

CREATE TABLE IF NOT EXISTS fact_incidents (
    incident_id UUID PRIMARY KEY,
    date_id DATE REFERENCES dim_date(date_id),
    location_id INT REFERENCES dim_location(location_id),
    vehicle_id INT REFERENCES dim_vehicle(vehicle_id),  
    contributing_factor_id INT REFERENCES dim_contributing_factor(factor_id),  
    number_of_persons_injured INT,
    number_of_persons_killed INT,
    number_of_motorist_injured INT,
    number_of_motorist_killed INT,
    number_of_pedestrians_injured INT,
    number_of_pedestrians_killed INT,
    number_of_cyclist_injured INT,
    number_of_cyclist_killed INT,
    number_of_incidents INT DEFAULT 1
);

CREATE INDEX idx_fact_date ON fact_incidents(date_id);
CREATE INDEX idx_fact_location ON fact_incidents(location_id);
CREATE INDEX idx_fact_vehicle ON fact_incidents(vehicle_id);
CREATE INDEX idx_fact_factor ON fact_incidents(contributing_factor_id);

CREATE MATERIALIZED VIEW mv_incidents_by_borough_month AS
SELECT
  d.borough,
  dd.month,
  dd.year,
  SUM(fi.number_of_persons_injured) AS total_injured,
  SUM(fi.number_of_persons_killed) AS total_killed
FROM fact_incidents fi
JOIN dim_location d ON d.location_id = fi.location_id
JOIN dim_date dd ON dd.date_id = fi.date_id
GROUP BY d.borough, dd.month, dd.year;

CREATE MATERIALIZED VIEW mv_top_streets_by_injuries AS
SELECT
  dl.on_street_name,
  SUM(fi.number_of_persons_injured) AS total_injured
FROM fact_incidents fi
JOIN dim_location dl ON fi.location_id = dl.location_id
GROUP BY dl.on_street_name
ORDER BY total_injured DESC
LIMIT 10;

CREATE MATERIALIZED VIEW mv_vehicle_type_incidents AS
SELECT
  dv.vehicle_type,
  dv.vehicle_position,
  COUNT(fi.incident_id) AS total_incidents,
  SUM(fi.number_of_persons_killed) AS total_killed
FROM fact_incidents fi
JOIN dim_vehicle dv ON fi.vehicle_id = dv.vehicle_id
GROUP BY dv.vehicle_type, dv.vehicle_position;

REFRESH MATERIALIZED VIEW mv_incidents_by_borough_month;
REFRESH MATERIALIZED VIEW mv_top_streets_by_injuries;
REFRESH MATERIALIZED VIEW mv_vehicle_type_incidents;




--for null values INSERT INTO dim_contributing_factor (factor_description, factor_position) VALUES ('UNKNOWN', 1);

