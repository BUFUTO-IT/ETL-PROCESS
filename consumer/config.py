# consumer/config.py
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
class DatabaseConfig:
    """Configuración de bases de datos"""
    # PostgreSQL
    postgres_host: str = os.getenv('POSTGRES_HOST', 'localhost')
    postgres_port: int = int(os.getenv('POSTGRES_PORT', 5432))
    postgres_db: str = os.getenv('POSTGRES_DB', 'iot_monitoring')
    postgres_user: str = os.getenv('POSTGRES_USER', 'iot_user')
    postgres_password: str = os.getenv('POSTGRES_PASSWORD', 'iot_password')
    
    # Redis
    redis_host: str = os.getenv('REDIS_HOST', 'localhost')
    redis_port: int = int(os.getenv('REDIS_PORT', 6379))
    redis_password: str = os.getenv('REDIS_PASSWORD', 'iot_password')
    redis_db: int = int(os.getenv('REDIS_DB', 0))
    
    @property
    def postgres_url(self):
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

@dataclass
class ConsumerConfig:
    """Configuración del consumidor"""
    max_retries: int = 3
    retry_delay: float = 5.0
    prefetch_count: int = 1
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')