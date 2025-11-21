import requests
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import json
from dataclasses import asdict

logger = logging.getLogger(__name__)

class SensorDataAPISender:
    """
    Cliente para enviar datos de sensores a un endpoint externo
    """
    
    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: int = 30):
        # ✅ CORREGIDO: Asegurar que la URL tenga protocolo
        if not base_url.startswith(('http://', 'https://')):
            base_url = f"http://{base_url}"  # Agregar http:// por defecto
            
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "SensorDataETL/1.0"
        }
        
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"
    
    def send_unified_sensor_data(self, sensor_data: Dict[str, List[Any]]) -> bool:
        """
        Envía datos unificados de los 3 sensores en un solo payload
        """
        try:
            # Preparar payload unificado
            payload = self._prepare_unified_payload(sensor_data)
            
            # Endpoint para datos unificados
            endpoint = f"{self.base_url}/api/sensor-data/unified"
            
            logger.info(f"📤 Enviando datos unificados al endpoint: {endpoint}")
            logger.info(f"   - Calidad de aire: {len(sensor_data.get('air_quality', []))} registros")
            logger.info(f"   - Sonido: {len(sensor_data.get('sound', []))} registros")
            logger.info(f"   - Agua: {len(sensor_data.get('water', []))} registros")
            
            # ✅ CORREGIDO: Mejor logging del payload
            logger.debug(f"Payload tamaño: {len(str(payload))} caracteres")
            
            response = requests.post(
                endpoint,
                json=payload,
                headers=self.headers,
                timeout=self.timeout
            )
            
            if response.status_code in [200, 201]:
                logger.info("✅ Datos unificados enviados exitosamente")
                return True
            else:
                logger.error(f"❌ Error del endpoint: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error de conexión: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error inesperado enviando datos: {e}")
            return False
    
    def send_individual_sensor_data(self, sensor_type: str, data: List[Any]) -> bool:
        """
        Envía datos de un solo tipo de sensor
        """
        try:
            # Convertir objetos a diccionarios
            records = [asdict(item) if hasattr(item, '__dataclass_fields__') else item for item in data]
            
            payload = {
                "sensor_type": sensor_type,
                "records": records,
                "timestamp": datetime.now().isoformat(),
                "total_records": len(records)
            }
            
            endpoint = f"{self.base_url}/api/sensor-data/{sensor_type}"
            
            logger.info(f"📤 Enviando {len(records)} registros de {sensor_type} a {endpoint}")
            
            response = requests.post(
                endpoint,
                json=payload,
                headers=self.headers,
                timeout=self.timeout
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ Datos de {sensor_type} enviados exitosamente")
                return True
            else:
                logger.error(f"❌ Error enviando {sensor_type}: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error enviando {sensor_type}: {e}")
            return False
    
    def _prepare_unified_payload(self, sensor_data: Dict[str, List[Any]]) -> Dict[str, Any]:
        """
        Prepara el payload unificado para los 3 sensores
        """
        # Convertir todos los objetos a diccionarios
        converted_data = {}
        total_records = 0
        
        for sensor_type, records in sensor_data.items():
            if records:  # ✅ CORREGIDO: Solo procesar si hay registros
                converted_records = [
                    asdict(item) if hasattr(item, '__dataclass_fields__') else item 
                    for item in records
                ]
                converted_data[sensor_type] = converted_records
                total_records += len(converted_records)
        
        payload = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "total_records": total_records,
                "source": "kafka_etl_processor",
                "version": "1.0"
            },
            "sensor_data": converted_data
        }
        
        return payload
    
    def health_check(self) -> bool:
        """
        Verifica que el endpoint esté disponible
        """
        try:
            # ✅ CORREGIDO: Endpoint con protocolo
            endpoint = f"{self.base_url}/health"
            logger.debug(f"🔍 Health check a: {endpoint}")
            response = requests.get(endpoint, timeout=10, headers=self.headers)
            is_healthy = response.status_code == 200
            logger.debug(f"Health check resultado: {is_healthy}")
            return is_healthy
        except Exception as e:
            logger.debug(f"Health check falló: {e}")
            return False