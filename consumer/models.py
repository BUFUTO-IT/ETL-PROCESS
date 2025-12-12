# consumer/models.py
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean, Text, CheckConstraint, ForeignKey, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime
import json

Base = declarative_base()

class Device(Base):
    """Modelo para dispositivos IoT"""
    __tablename__ = 'devices'
    
    id = Column(Integer, primary_key=True)
    device_name = Column(String(100), unique=True, nullable=False, index=True)
    sensor_type = Column(String(50), nullable=False)
    latitude = Column(DECIMAL(10, 8))  # CAMBIADO: Float -> DECIMAL
    longitude = Column(DECIMAL(11, 8))  # CAMBIADO: Float -> DECIMAL
    address = Column(Text)
    installation_date = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime)
    is_active = Column(Boolean, default=True)
    battery_level = Column(DECIMAL(5, 2))  # CAMBIADO: Float -> DECIMAL
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relaciones
    air_measurements = relationship("AirMeasurement", backref="device", cascade="all, delete-orphan")
    sound_measurements = relationship("SoundMeasurement", backref="device", cascade="all, delete-orphan")
    water_measurements = relationship("WaterMeasurement", backref="device", cascade="all, delete-orphan")
    alerts = relationship("Alert", backref="device", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Device {self.device_name} ({self.sensor_type})>"
    
    def to_dict(self):
        return {
            'id': self.id,
            'device_name': self.device_name,
            'sensor_type': self.sensor_type,
            'latitude': float(self.latitude) if self.latitude else None,
            'longitude': float(self.longitude) if self.longitude else None,
            'is_active': self.is_active,
            'battery_level': float(self.battery_level) if self.battery_level else None,
            'last_seen': self.last_seen.isoformat() if self.last_seen else None
        }

class AirMeasurement(Base):
    """Modelo para mediciones de aire - ACTUALIZADO"""
    __tablename__ = 'air_measurements'
    
    id = Column(Integer, primary_key=True)
    device_name = Column(String(100), ForeignKey('devices.device_name'), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    latitude = Column(DECIMAL(10, 8))  # CAMBIADO
    longitude = Column(DECIMAL(11, 8))  # CAMBIADO
    
    # Mediciones
    co2 = Column(DECIMAL(6, 2))  # CAMBIADO: Float -> DECIMAL
    temperature = Column(DECIMAL(4, 2))  # CAMBIADO
    humidity = Column(DECIMAL(5, 2))  # CAMBIADO
    pressure = Column(DECIMAL(7, 2))  # CAMBIADO
    battery = Column(DECIMAL(5, 2))  # CAMBIADO
    
    # Calidad de señal - AÑADIDO (faltaba)
    rssi = Column(DECIMAL(5, 2))  # AÑADIDO: Esta columna existe en SQL
    
    data_quality = Column(String(20), default='good')
    
    # Categorías calculadas
    air_quality_category = Column(String(20))
    temperature_category = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        CheckConstraint('co2 BETWEEN 300 AND 5000 OR co2 IS NULL', name='check_co2_range'),
        CheckConstraint('temperature BETWEEN -10 AND 50 OR temperature IS NULL', name='check_temp_range'),
        CheckConstraint('humidity BETWEEN 0 AND 100 OR humidity IS NULL', name='check_humidity_range'),
        CheckConstraint('pressure BETWEEN 500 AND 1100 OR pressure IS NULL', name='check_pressure_range'),  # AÑADIDO
        CheckConstraint('battery BETWEEN 0 AND 100 OR battery IS NULL', name='check_battery_range'),  # AÑADIDO
    )
    
    def to_dict(self):
        return {
            'id': self.id,
            'device_name': self.device_name,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'co2': float(self.co2) if self.co2 else None,
            'temperature': float(self.temperature) if self.temperature else None,
            'humidity': float(self.humidity) if self.humidity else None,
            'pressure': float(self.pressure) if self.pressure else None,
            'battery': float(self.battery) if self.battery else None,
            'rssi': float(self.rssi) if self.rssi else None,  # AÑADIDO
            'air_quality_category': self.air_quality_category,
            'temperature_category': self.temperature_category,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

class SoundMeasurement(Base):
    """Modelo para mediciones de sonido"""
    __tablename__ = 'sound_measurements'
    
    id = Column(Integer, primary_key=True)
    device_name = Column(String(100), ForeignKey('devices.device_name'), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    
    # Mediciones
    laeq = Column(Float)  # dB
    lai = Column(Float)  # dB
    laimax = Column(Float)  # dB
    battery = Column(Float)  # %
    
    # Estado
    status = Column(String(50))
    data_quality = Column(String(20), default='good')
    
    # Categorías calculadas
    noise_category = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        CheckConstraint('laeq BETWEEN 30 AND 120 OR laeq IS NULL', name='check_laeq_range'),
    )
    
    def to_dict(self):
        return {
            'id': self.id,
            'device_name': self.device_name,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'laeq': self.laeq,
            'lai': self.lai,
            'laimax': self.laimax,
            'battery': self.battery,
            'noise_category': self.noise_category,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

class WaterMeasurement(Base):
    """Modelo para mediciones de agua"""
    __tablename__ = 'water_measurements'
    
    id = Column(Integer, primary_key=True)
    device_name = Column(String(100), ForeignKey('devices.device_name'), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    
    # Mediciones
    water_level = Column(Float)  # %
    distance = Column(Float)  # cm
    battery = Column(Float)  # %
    
    # Estado
    status = Column(String(50))
    code = Column(String(50))
    data_quality = Column(String(20), default='good')
    
    # Categorías calculadas
    tank_status = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        CheckConstraint('water_level BETWEEN 0 AND 100 OR water_level IS NULL', name='check_water_level_range'),
    )
    
    def to_dict(self):
        return {
            'id': self.id,
            'device_name': self.device_name,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'water_level': self.water_level,
            'distance': self.distance,
            'battery': self.battery,
            'tank_status': self.tank_status,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

# models.py - CORREGIR LA CLASE Alert
class Alert(Base):
    """Modelo para alertas"""
    __tablename__ = 'alerts'
    
    id = Column(Integer, primary_key=True)
    device_name = Column(String(100), ForeignKey('devices.device_name'), nullable=False, index=True)
    sensor_type = Column(String(50), nullable=False)
    alert_type = Column(String(50), nullable=False)
    message = Column(Text, nullable=False)
    value = Column(DECIMAL(10, 2))  # CAMBIADO: Float -> DECIMAL
    threshold = Column(DECIMAL(10, 2))  # CAMBIADO: Float -> DECIMAL
    severity = Column(String(20))
    
    # Columnas específicas por tipo de medición
    air_measurement_id = Column(Integer, ForeignKey('air_measurements.id'))
    sound_measurement_id = Column(Integer, ForeignKey('sound_measurements.id'))
    water_measurement_id = Column(Integer, ForeignKey('water_measurements.id'))
    
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    is_resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        CheckConstraint("severity IN ('low', 'medium', 'high')", name='check_severity'),
    )
    
    def to_dict(self):
        return {
            'id': self.id,
            'device_name': self.device_name,
            'sensor_type': self.sensor_type,
            'alert_type': self.alert_type,
            'message': self.message,
            'value': float(self.value) if self.value else None,
            'threshold': float(self.threshold) if self.threshold else None,
            'severity': self.severity,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'is_resolved': self.is_resolved,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }