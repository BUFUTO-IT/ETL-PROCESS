import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, Any, List
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class PostgreSQLManager:
    def __init__(self, dbname=None, user=None, password=None, host=None, port=5432):
        self.connection_params = {
            'dbname': dbname or os.getenv('POSTGRES_DB', 'sensor_etl'),
            'user': user or os.getenv('POSTGRES_USER', 'postgres'),
            'password': password or os.getenv('POSTGRES_PASSWORD', 'password'),
            'host': host or os.getenv('POSTGRES_HOST', 'localhost'),
            'port': port or int(os.getenv('POSTGRES_PORT', 5432))
        }
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establece conexión con PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("✅ Conectado a PostgreSQL")
        except Exception as e:
            logger.error(f"❌ Error conectando a PostgreSQL: {e}")
            raise
    
    def insert_sensor_data(self, sensor_type: str, data: Dict[str, Any]) -> bool:
        """Inserta datos de sensor en la base de datos"""
        try:
            with self.conn.cursor() as cur:
                # Insertar en tabla base
                base_query = """
                INSERT INTO sensor_base_data 
                (_id, dev_addr, deduplication_id, time, device_class, tenant_name, 
                 device_name, location, latitude, longitude, battery_level, fcnt, 
                 rssi, snr, sensor_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """
                
                base_data = (
                    data.get('_id'),
                    data.get('devAddr'),
                    data.get('deduplicationId'),
                    data.get('time'),
                    data.get('device_class'),
                    data.get('tenant_name'),
                    data.get('device_name'),
                    data.get('location'),
                    data.get('latitude'),
                    data.get('longitude'),
                    data.get('battery_level'),
                    data.get('fCnt'),
                    data.get('rssi'),
                    data.get('snr'),
                    sensor_type
                )
                
                cur.execute(base_query, base_data)
                base_id = cur.fetchone()[0]
                
                # Insertar en tabla específica según el tipo de sensor
                if sensor_type == 'air_quality':
                    sensor_query = """
                    INSERT INTO air_quality_data 
                    (sensor_base_id, co2, temperature, humidity, pressure, 
                     co2_status, temperature_status, humidity_status, pressure_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    sensor_data = (
                        base_id,
                        data.get('co2'),
                        data.get('temperature'),
                        data.get('humidity'),
                        data.get('pressure'),
                        data.get('co2_status'),
                        data.get('temperature_status'),
                        data.get('humidity_status'),
                        data.get('pressure_status')
                    )
                    
                elif sensor_type == 'sound':
                    sensor_query = """
                    INSERT INTO sound_data 
                    (sensor_base_id, laeq, lai, laimax, sound_status)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    sensor_data = (
                        base_id,
                        data.get('laeq'),
                        data.get('lai'),
                        data.get('laimax'),
                        data.get('sound_status')
                    )
                    
                elif sensor_type == 'water':
                    sensor_query = """
                    INSERT INTO water_data 
                    (sensor_base_id, distance, position, water_status)
                    VALUES (%s, %s, %s, %s)
                    """
                    sensor_data = (
                        base_id,
                        data.get('distance'),
                        data.get('position'),
                        data.get('water_status')
                    )
                else:
                    logger.warning(f"⚠️ Tipo de sensor no soportado: {sensor_type}")
                    self.conn.rollback()
                    return False
                
                cur.execute(sensor_query, sensor_data)
                self.conn.commit()
                logger.debug(f"💾 Datos insertados en PostgreSQL: {sensor_type} - {data.get('_id')}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error insertando en PostgreSQL: {e}")
            self.conn.rollback()
            return False
    
    def batch_insert_sensor_data(self, sensor_type: str, data_list: List[Dict[str, Any]]) -> int:
        """Inserta múltiples registros en lote"""
        success_count = 0
        for data in data_list:
            if self.insert_sensor_data(sensor_type, data):
                success_count += 1
        return success_count
    
    def get_sensor_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de la base de datos"""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Conteo por tipo de sensor
                cur.execute("""
                    SELECT sensor_type, COUNT(*) as count 
                    FROM sensor_base_data 
                    GROUP BY sensor_type
                """)
                type_counts = cur.fetchall()
                
                # Total de registros
                cur.execute("SELECT COUNT(*) as total FROM sensor_base_data")
                total = cur.fetchone()['total']
                
                return {
                    'total_records': total,
                    'records_by_type': {row['sensor_type']: row['count'] for row in type_counts},
                    'database': self.connection_params['dbname']
                }
                
        except Exception as e:
            logger.error(f"❌ Error obteniendo estadísticas: {e}")
            return {}
    
    def close(self):
        """Cierra la conexión"""
        if self.conn:
            self.conn.close()
            logger.info("🔚 Conexión PostgreSQL cerrada")