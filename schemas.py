from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime

@dataclass
class BaseSensorData:
    """Esquema base para todos los sensores"""
    _id: str
    devAddr: str
    deduplicationId: str
    time: datetime
    device_class: str
    tenant_name: str
    device_name: str
    location: str
    latitude: Optional[float]
    longitude: Optional[float]
    battery_level: Optional[float]
    fCnt: int
    rssi: Optional[float]
    snr: Optional[float]

@dataclass
class AirQualityData(BaseSensorData):
    """Esquema para datos de calidad de aire"""
    co2: Optional[float]
    temperature: Optional[float]
    humidity: Optional[float]
    pressure: Optional[float]
    co2_status: Optional[str]
    temperature_status: Optional[str]
    humidity_status: Optional[str]
    pressure_status: Optional[str]

@dataclass
class SoundData(BaseSensorData):
    """Esquema para datos de sonido"""
    laeq: Optional[float]
    lai: Optional[float]
    laimax: Optional[float]
    sound_status: Optional[str]

@dataclass
class WaterData(BaseSensorData):
    """Esquema para datos de agua"""
    distance: Optional[float]
    position: Optional[str]
    water_status: Optional[str]