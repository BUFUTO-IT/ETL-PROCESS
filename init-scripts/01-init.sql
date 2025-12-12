-- init-scripts/01-init.sql - ESQUEMA COMPLETO CORREGIDO
-- Crear tablas principales

-- Tabla de dispositivos
CREATE TABLE IF NOT EXISTS devices (
    id SERIAL PRIMARY KEY,
    device_name VARCHAR(100) UNIQUE NOT NULL,
    sensor_type VARCHAR(50) NOT NULL CHECK (sensor_type IN ('aire', 'sonido', 'agua')),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    address TEXT,
    installation_date TIMESTAMPTZ DEFAULT NOW(),
    last_seen TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT TRUE,
    battery_level DECIMAL(5, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Tabla de mediciones de aire
CREATE TABLE IF NOT EXISTS air_measurements (
    id SERIAL PRIMARY KEY,
    device_name VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    
    -- Mediciones principales
    co2 DECIMAL(6, 2) CHECK (co2 BETWEEN 300 AND 5000 OR co2 IS NULL),
    temperature DECIMAL(4, 2) CHECK (temperature BETWEEN -10 AND 50 OR temperature IS NULL),
    humidity DECIMAL(5, 2) CHECK (humidity BETWEEN 0 AND 100 OR humidity IS NULL),
    pressure DECIMAL(7, 2) CHECK (pressure BETWEEN 500 AND 1100 OR pressure IS NULL),
    battery DECIMAL(5, 2) CHECK (battery BETWEEN 0 AND 100 OR battery IS NULL),
    
    -- Calidad de señal
    rssi DECIMAL(5, 2),
    data_quality VARCHAR(20) DEFAULT 'good',
    
    -- Categorías calculadas
    air_quality_category VARCHAR(20),
    temperature_category VARCHAR(20),
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign key
    FOREIGN KEY (device_name) REFERENCES devices(device_name) ON DELETE CASCADE
);

-- Tabla de mediciones de sonido
CREATE TABLE IF NOT EXISTS sound_measurements (
    id SERIAL PRIMARY KEY,
    device_name VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    
    -- Mediciones principales
    laeq DECIMAL(5, 2) CHECK (laeq BETWEEN 30 AND 120 OR laeq IS NULL),
    lai DECIMAL(5, 2),
    laimax DECIMAL(5, 2),
    battery DECIMAL(5, 2) CHECK (battery BETWEEN 0 AND 100 OR battery IS NULL),
    
    -- Estado
    status VARCHAR(50),
    data_quality VARCHAR(20) DEFAULT 'good',
    
    -- Categorías calculadas
    noise_category VARCHAR(20),
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    FOREIGN KEY (device_name) REFERENCES devices(device_name) ON DELETE CASCADE
);

-- Tabla de mediciones de agua
CREATE TABLE IF NOT EXISTS water_measurements (
    id SERIAL PRIMARY KEY,
    device_name VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    
    -- Mediciones principales
    water_level DECIMAL(5, 2) CHECK (water_level BETWEEN 0 AND 100 OR water_level IS NULL),
    distance DECIMAL(6, 2),
    battery DECIMAL(5, 2) CHECK (battery BETWEEN 0 AND 100 OR battery IS NULL),
    
    -- Estado
    status VARCHAR(50),
    code VARCHAR(50),
    data_quality VARCHAR(20) DEFAULT 'good',
    
    -- Categorías calculadas
    tank_status VARCHAR(20),
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    FOREIGN KEY (device_name) REFERENCES devices(device_name) ON DELETE CASCADE
);

-- ✅ CORREGIDO: Tabla de alertas con referencias específicas por tipo de medición
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    device_name VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    value DECIMAL(10, 2),
    threshold DECIMAL(10, 2),
    severity VARCHAR(20) CHECK (severity IN ('low', 'medium', 'high')),
    
    -- ✅ NUEVO: Referencias específicas a cada tipo de medición
    air_measurement_id INTEGER,
    sound_measurement_id INTEGER,
    water_measurement_id INTEGER,
    
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Foreign keys
    FOREIGN KEY (device_name) REFERENCES devices(device_name) ON DELETE CASCADE,
    FOREIGN KEY (air_measurement_id) REFERENCES air_measurements(id) ON DELETE SET NULL,
    FOREIGN KEY (sound_measurement_id) REFERENCES sound_measurements(id) ON DELETE SET NULL,
    FOREIGN KEY (water_measurement_id) REFERENCES water_measurements(id) ON DELETE SET NULL,
    
    -- ✅ NUEVO: Constraint para asegurar que solo un measurement_id esté presente
    CHECK (
        (air_measurement_id IS NOT NULL AND sound_measurement_id IS NULL AND water_measurement_id IS NULL) OR
        (air_measurement_id IS NULL AND sound_measurement_id IS NOT NULL AND water_measurement_id IS NULL) OR
        (air_measurement_id IS NULL AND sound_measurement_id IS NULL AND water_measurement_id IS NOT NULL)
    )
);

-- Índices para mejor rendimiento
CREATE INDEX IF NOT EXISTS idx_air_timestamp ON air_measurements(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_air_device ON air_measurements(device_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sound_timestamp ON sound_measurements(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sound_device ON sound_measurements(device_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_water_timestamp ON water_measurements(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_water_device ON water_measurements(device_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_device ON alerts(device_name);
CREATE INDEX IF NOT EXISTS idx_alerts_resolved ON alerts(is_resolved, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_devices_active ON devices(is_active, sensor_type);

-- ✅ NUEVO: Índices para las FK de alertas
CREATE INDEX IF NOT EXISTS idx_alerts_air_measurement ON alerts(air_measurement_id);
CREATE INDEX IF NOT EXISTS idx_alerts_sound_measurement ON alerts(sound_measurement_id);
CREATE INDEX IF NOT EXISTS idx_alerts_water_measurement ON alerts(water_measurement_id);

-- Vista para dashboard
CREATE OR REPLACE VIEW device_status AS
SELECT 
    d.device_name,
    d.sensor_type,
    d.latitude,
    d.longitude,
    d.is_active,
    d.battery_level,
    d.last_seen,
    CASE 
        WHEN d.last_seen < NOW() - INTERVAL '1 hour' THEN 'offline'
        ELSE 'online'
    END as connection_status,
    CASE d.sensor_type
        WHEN 'aire' THEN (SELECT air_quality_category FROM air_measurements WHERE device_name = d.device_name ORDER BY timestamp DESC LIMIT 1)
        WHEN 'sonido' THEN (SELECT noise_category FROM sound_measurements WHERE device_name = d.device_name ORDER BY timestamp DESC LIMIT 1)
        WHEN 'agua' THEN (SELECT tank_status FROM water_measurements WHERE device_name = d.device_name ORDER BY timestamp DESC LIMIT 1)
        ELSE NULL
    END as last_status
FROM devices d;

-- ✅ MANTENIDO: Función y triggers para actualizar last_seen automáticamente
-- Nota: Si prefieres manejar esto en Python, puedes comentar esta sección
CREATE OR REPLACE FUNCTION update_device_last_seen()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE devices 
    SET last_seen = NEW.timestamp,
        updated_at = NOW()
    WHERE device_name = NEW.device_name;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers para actualizar last_seen automáticamente
CREATE TRIGGER update_air_device_last_seen
    AFTER INSERT ON air_measurements
    FOR EACH ROW
    EXECUTE FUNCTION update_device_last_seen();

CREATE TRIGGER update_sound_device_last_seen
    AFTER INSERT ON sound_measurements
    FOR EACH ROW
    EXECUTE FUNCTION update_device_last_seen();

CREATE TRIGGER update_water_device_last_seen
    AFTER INSERT ON water_measurements
    FOR EACH ROW
    EXECUTE FUNCTION update_device_last_seen();

-- En 01-init.sql, añadir constraint de batería que falta:
ALTER TABLE air_measurements 
ADD CONSTRAINT check_battery_range 
CHECK (battery BETWEEN 0 AND 100 OR battery IS NULL);

ALTER TABLE sound_measurements 
ADD CONSTRAINT check_sound_battery_range 
CHECK (battery BETWEEN 0 AND 100 OR battery IS NULL);

ALTER TABLE water_measurements 
ADD CONSTRAINT check_water_battery_range 
CHECK (battery BETWEEN 0 AND 100 OR battery IS NULL);