CREATE TABLE IF NOT EXISTS weather_readings (
    id BIGSERIAL PRIMARY KEY,
    station_id BIGINT NOT NULL,
    s_no BIGINT NOT NULL,
    battery_status VARCHAR(10) NOT NULL,
    status_timestamp BIGINT NOT NULL,
    humidity INT NOT NULL,
    temperature INT NOT NULL,
    wind_speed INT NOT NULL
);
