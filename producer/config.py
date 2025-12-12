# producer/config.py
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class RabbitMQConfig:
    """Configuración de RabbitMQ"""
    host: str = os.getenv('RABBITMQ_HOST', 'localhost')
    port: int = int(os.getenv('RABBITMQ_PORT', 5672))
    username: str = os.getenv('RABBITMQ_USER', 'iot_user')
    password: str = os.getenv('RABBITMQ_PASSWORD', 'iot_password')
    
    @property
    def queue_names(self):
        return {
            'aire': 'sensor.aire',
            'sonido': 'sensor.sonido',
            'agua': 'sensor.agua'
        }

@dataclass
class ProducerConfig:
    """Configuración del productor"""
    batch_size: int = int(os.getenv('BATCH_SIZE', 50))
    sleep_interval: float = float(os.getenv('SLEEP_INTERVAL', 2.0))
    dataset_path: str = os.getenv('DATASET_PATH', 'datasets')
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')