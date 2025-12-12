# config.py CORREGIDO
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass  # ← SOLO UNO
class RedisConfig:
    host: str = os.getenv('REDIS_HOST', 'localhost')
    port: int = int(os.getenv('REDIS_PORT', 6379))
    password: str = os.getenv('REDIS_PASSWORD', 'iot_password')
    db: int = int(os.getenv('REDIS_DB', 0))
    socket_timeout: int = int(os.getenv('REDIS_SOCKET_TIMEOUT', 30))
    socket_connect_timeout: int = int(os.getenv('REDIS_CONNECT_TIMEOUT', 10))

@dataclass
class APIConfig:
    host: str = os.getenv('API_HOST', '0.0.0.0')
    port: int = int(os.getenv('API_PORT', 8000))
    debug: bool = os.getenv('DEBUG', 'False').lower() == 'true'

@dataclass
class MLConfig:
    models_path: str = os.getenv('MODELS_PATH', 'models')
    prediction_days: int = int(os.getenv('PREDICTION_DAYS', 7))
    min_samples: int = int(os.getenv('MIN_TRAINING_SAMPLES', 50))
    debug: bool = os.getenv('DEBUG', 'False').lower() == 'true'  # Agregar esta línea