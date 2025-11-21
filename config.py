import os
from dotenv import load_dotenv

load_dotenv()

class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    TOPIC_AIR_QUALITY = 'sensor-air-quality'
    TOPIC_SOUND = 'sensor-sound'
    TOPIC_WATER = 'sensor-water'
    GROUP_ID = 'sensor-etl-group'
    
    # Configuración para KAFKA-PYTHON (no confluent-kafka)
    PRODUCER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,  # Con guión bajo, no punto
        'client_id': 'sensor-producer',           # client_id, no client.id
        'acks': 'all',
        'retries': 3,
        'batch_size': 16384,      # batch_size, no batch.size
        'linger_ms': 1,           # linger_ms, no linger.ms
        'buffer_memory': 33554432 # buffer_memory, no buffer.memory
    }
    
    # Configuración para KAFKA-PYTHON
    CONSUMER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'group_id': GROUP_ID,           # group_id, no group.id
        'auto_offset_reset': 'earliest', # auto_offset_reset, no auto.offset.reset
        'enable_auto_commit': False,     # enable_auto_commit, no enable.auto.commit
        'max_poll_records': 500,         # max_poll_records, no max.poll.records
        'session_timeout_ms': 30000      # session_timeout_ms, no session.timeout.ms
    }

class ETLConfig:
    # Configuración de procesamiento
    BATCH_SIZE = 100
    PROCESSING_DELAY = 0.01
    
    # Umbrales de validación de señal
    MIN_RSSI = -130
    MAX_RSSI = 0
    MIN_SNR = -20
    MAX_SNR = 20
    
    # 🛡️ NUEVO: Límites de calidad de datos
    MAX_NULL_PERCENTAGE = 60.0  # Máximo 60% de campos nulos
    MIN_REQUIRED_FIELDS = 3     # Mínimo 3 campos requeridos presentes
    
    # 🛡️ NUEVO: Rangos de validación por sensor
    SENSOR_RANGES = {
        'air_quality': {
            'co2': (300, 5000),
            'temperature': (-50, 60),
            'humidity': (0, 100),
            'pressure': (500, 1100)
        },
        'sound': {
            'laeq': (30, 120),
            'lai': (30, 120),
            'laimax': (30, 120)
        },
        'water': {
            'distance': (0, 100)
        }
    }
    
    # Rutas de salida
    OUTPUT_DIR = 'processed_data'
    LOG_DIR = 'logs'

# Añadir al final de tu config.py
class APIConfig:
    """Configuración para el API sender"""
    # ✅ CORREGIDO: URL con protocolo
    BASE_URL = "http://localhost:8000"  # Ahora con http://
    
    # API Key si es requerida
    API_KEY = "emergentes"  # Opcional
    
    # Timeout en segundos
    TIMEOUT = 30
    
    # Intentos de reintento
    RETRY_ATTEMPTS = 3
    
    # Habilitar/deshabilitar envío
    ENABLED = True