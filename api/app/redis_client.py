import redis
# Agregar al inicio del archivo:
import time  # ← AGREGAR ESTO
import json
import logging
from typing import List, Dict, Optional
from app.config import RedisConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self, config: RedisConfig):
        self.config = config
        self.client = None
        self.retry_attempts = 2
        self.retry_delay = 1
        
    def connect(self):
        logger.info(f"Intentando conectar a Redis en {self.config.host}:{self.config.port}")
        
        for attempt in range(self.retry_attempts):
            try:
                self.client = redis.Redis(
                    host=self.config.host,
                    port=self.config.port,
                    password=self.config.password if self.config.password else None,
                    db=self.config.db,
                    decode_responses=True,
                    socket_connect_timeout=5,  # Timeout más corto
                    socket_timeout=5,
                    retry_on_timeout=False,  # Desactivar reintentos automáticos
                    max_connections=5
                )
                
                # Test de conexión
                if self.client.ping():
                    logger.info(f"✅ Conectado a Redis")
                    return True
                else:
                    logger.warning(f"Ping falló en intento {attempt + 1}")
                    
            except redis.ConnectionError as e:
                logger.warning(f"Intento {attempt + 1} fallado: {e}")
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"Error inesperado: {e}")
                return False
        
        logger.error("❌ No se pudo conectar a Redis después de todos los intentos")
        self.client = None
        return False
    
    def get_active_devices(self, sensor_type: str) -> List[str]:
        """Obtener dispositivos activos con manejo de errores"""
        if not self.client:
            logger.warning("Redis no conectado")
            return []
        
        try:
            active_key = f"active_devices:{sensor_type}"
            devices = self.client.smembers(active_key)
            logger.info(f"Encontrados {len(devices)} dispositivos para {sensor_type}")
            return list(devices)
        except Exception as e:
            logger.error(f"Error obteniendo dispositivos: {e}")
            return []
    
    
    def get_device_data(self, device_name: str) -> Dict:
        """Obtener datos actuales de un dispositivo"""
        try:
            device_key = f"device:{device_name}"
            data = self.client.hgetall(device_key)
            data['device_name'] = device_name
            return data
        except Exception as e:
            logger.error(f"Error obteniendo datos de dispositivo: {e}")
            return {}
    
    def get_device_history(self, sensor_type: str, device_name: str, limit: int = 100) -> List[Dict]:
        """Obtener historial de un dispositivo"""
        try:
            history_key = f"history:{sensor_type}:{device_name}"
            history_data = self.client.lrange(history_key, 0, limit - 1)
            
            measurements = []
            for item in history_data:
                try:
                    record = json.loads(item)
                    
                    # NUEVA LÓGICA: Si no hay campo 'data', crear uno con todos los campos excepto timestamp
                    if 'data' not in record:
                        data_fields = {k: v for k, v in record.items() if k != 'timestamp'}
                        record['data'] = data_fields
                    # Si 'data' es string, parsearlo
                    elif isinstance(record.get('data'), str):
                        record['data'] = json.loads(record['data'])
                        
                    measurements.append(record)
                except Exception as e:
                    logger.warning(f"Error parseando registro: {e}")
                    continue
            
            return measurements
        except Exception as e:
            logger.error(f"Error obteniendo historial: {e}")
            return []
    
    def get_all_sensor_data(self, sensor_type: str, limit_per_device: int = 50) -> List[Dict]:
        """Obtener todos los datos de un tipo de sensor"""
        try:
            devices = self.get_active_devices(sensor_type)
            all_data = []
            
            for device_name in devices:
                history = self.get_device_history(sensor_type, device_name, limit_per_device)
                for record in history:
                    record['device_name'] = device_name
                    all_data.append(record)
            
            # Ordenar por timestamp
            all_data.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            return all_data
        except Exception as e:
            logger.error(f"Error obteniendo datos de sensor: {e}")
            return []
    
    def close(self):
        """Cerrar conexión"""
        if self.client:
            self.client.close()