-- Crear base de datos
CREATE DATABASE sensor_etl;

-- Conectarse a la base de datos sensor_etl

-- Tabla base para todos los sensores
CREATE TABLE sensor_base_data (
    id SERIAL PRIMARY KEY,
    _id VARCHAR(255) UNIQUE NOT NULL,
    dev_addr VARCHAR(100),
    deduplication_id VARCHAR(255),
    time TIMESTAMP NOT NULL,
    device_class VARCHAR(100),
    tenant_name VARCHAR(100),
    device_name VARCHAR(100),
    location TEXT,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    battery_level DECIMAL(8, 2),
    fcnt INTEGER,
    rssi DECIMAL(8, 2),
    snr DECIMAL(8, 2),
    sensor_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para datos de calidad de aire
CREATE TABLE air_quality_data (
    id SERIAL PRIMARY KEY,
    sensor_base_id INTEGER REFERENCES sensor_base_data(id),
    co2 DECIMAL(8, 2),
    temperature DECIMAL(6, 2),
    humidity DECIMAL(6, 2),
    pressure DECIMAL(8, 2),
    co2_status VARCHAR(50),
    temperature_status VARCHAR(50),
    humidity_status VARCHAR(50),
    pressure_status VARCHAR(50)
);

-- Tabla para datos de sonido
CREATE TABLE sound_data (
    id SERIAL PRIMARY KEY,
    sensor_base_id INTEGER REFERENCES sensor_base_data(id),
    laeq DECIMAL(6, 2),
    lai DECIMAL(6, 2),
    laimax DECIMAL(6, 2),
    sound_status VARCHAR(50)
);

-- Tabla para datos de agua
CREATE TABLE water_data (
    id SERIAL PRIMARY KEY,
    sensor_base_id INTEGER REFERENCES sensor_base_data(id),
    distance DECIMAL(6, 2),
    position VARCHAR(100),
    water_status VARCHAR(50)
);

-- Índices para mejorar el rendimiento
CREATE INDEX idx_sensor_base_time ON sensor_base_data(time);
CREATE INDEX idx_sensor_base_type ON sensor_base_data(sensor_type);
CREATE INDEX idx_sensor_base_device ON sensor_base_data(device_name);
CREATE INDEX idx_air_quality_co2 ON air_quality_data(co2);
CREATE INDEX idx_sound_laeq ON sound_data(laeq);

-- Vista para datos consolidados
CREATE VIEW sensor_consolidated_view AS
SELECT 
    b.*,
    a.co2, a.temperature, a.humidity, a.pressure,
    a.co2_status, a.temperature_status, a.humidity_status, a.pressure_status,
    s.laeq, s.lai, s.laimax, s.sound_status,
    w.distance, w.position, w.water_status
FROM sensor_base_data b
LEFT JOIN air_quality_data a ON b.id = a.sensor_base_id
LEFT JOIN sound_data s ON b.id = s.sensor_base_id
LEFT JOIN water_data w ON b.id = w.sensor_base_id;