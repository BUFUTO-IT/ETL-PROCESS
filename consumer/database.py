# consumer/database.py - VERSIÓN FINAL CORREGIDA
import redis
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import logging
from datetime import datetime, timedelta
from models import Base, Device, AirMeasurement, SoundMeasurement, WaterMeasurement, Alert
from config import DatabaseConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Gestor de conexiones a bases de datos"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.postgres_engine = None
        self.Session = None
        self.redis_client = None
        
    def connect_postgres(self):
        """Conectar a PostgreSQL con reintentos"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Conectando a PostgreSQL (intento {attempt + 1}/{max_retries})...")
                
                self.postgres_engine = create_engine(
                    self.config.postgres_url,
                    pool_size=10,
                    max_overflow=20,
                    pool_pre_ping=True,
                    echo=False
                )
                
                # Test connection
                with self.postgres_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                
                self.Session = sessionmaker(bind=self.postgres_engine)
                
                # Crear tablas si no existen
                Base.metadata.create_all(self.postgres_engine)
                
                logger.info("✅ Conectado a PostgreSQL exitosamente")
                return True
                
            except Exception as e:
                logger.error(f"Error conectando a PostgreSQL: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Reintentando en {retry_delay} segundos...")
                    import time
                    time.sleep(retry_delay)
                else:
                    logger.error("❌ No se pudo conectar a PostgreSQL después de varios intentos")
                    return False
    
    def connect_redis(self):
        """Conectar a Redis con reintentos"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Conectando a Redis (intento {attempt + 1}/{max_retries})...")
                
                self.redis_client = redis.Redis(
                    host=self.config.redis_host,
                    port=self.config.redis_port,
                    password=self.config.redis_password if self.config.redis_password else None,
                    db=self.config.redis_db,
                    decode_responses=False,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True
                )
                
                # Test connection
                self.redis_client.ping()
                
                logger.info("✅ Conectado a Redis exitosamente")
                return True
                
            except Exception as e:
                logger.error(f"Error conectando a Redis: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Reintentando en {retry_delay} segundos...")
                    import time
                    time.sleep(retry_delay)
                else:
                    logger.error("❌ No se pudo conectar a Redis después de varios intentos")
                    return False
    
    def save_to_postgres(self, sensor_type, data):
        """Guardar datos en PostgreSQL según el tipo de sensor"""
        try:
            session = self.Session()
            
            # Función auxiliar para convertir valores
            def to_decimal_or_none(value):
                if value is None:
                    return None
                try:
                    return float(value)
                except:
                    return None
            
            # Actualizar o crear dispositivo
            device = session.query(Device).filter_by(
                device_name=data['device_name']
            ).first()
            
            if not device:
                device = Device(
                    device_name=data['device_name'],
                    sensor_type=sensor_type,
                    latitude=to_decimal_or_none(data.get('latitude')),
                    longitude=to_decimal_or_none(data.get('longitude')),
                    last_seen=data.get('timestamp'),
                    battery_level=to_decimal_or_none(data.get('battery'))
                )
                session.add(device)
                logger.info(f"➕ Nuevo dispositivo creado: {data['device_name']}")
            else:
                device.last_seen = data.get('timestamp')
                if data.get('battery'):
                    device.battery_level = to_decimal_or_none(data['battery'])
                device.updated_at = datetime.utcnow()
            
            # Guardar medición según tipo
            measurement = None
            
            if sensor_type == 'aire':
                measurement = AirMeasurement(
                    device_name=data['device_name'],
                    timestamp=data.get('timestamp'),
                    latitude=to_decimal_or_none(data.get('latitude')),
                    longitude=to_decimal_or_none(data.get('longitude')),
                    co2=to_decimal_or_none(data.get('co2')),
                    temperature=to_decimal_or_none(data.get('temperature')),
                    humidity=to_decimal_or_none(data.get('humidity')),
                    pressure=to_decimal_or_none(data.get('pressure')),
                    battery=to_decimal_or_none(data.get('battery')),
                    air_quality_category=data.get('air_quality_category'),
                    temperature_category=data.get('temperature_category'),
                    data_quality=data.get('data_quality', 'good')
                )
                
            elif sensor_type == 'sonido':
                measurement = SoundMeasurement(
                    device_name=data['device_name'],
                    timestamp=data.get('timestamp'),
                    latitude=to_decimal_or_none(data.get('latitude')),
                    longitude=to_decimal_or_none(data.get('longitude')),
                    laeq=to_decimal_or_none(data.get('laeq')),
                    lai=to_decimal_or_none(data.get('lai')),
                    laimax=to_decimal_or_none(data.get('laimax')),
                    battery=to_decimal_or_none(data.get('battery')),
                    status=data.get('status'),
                    noise_category=data.get('noise_category'),
                    data_quality=data.get('data_quality', 'good')
                )
                
            elif sensor_type == 'agua':
                measurement = WaterMeasurement(
                    device_name=data['device_name'],
                    timestamp=data.get('timestamp'),
                    latitude=to_decimal_or_none(data.get('latitude')),
                    longitude=to_decimal_or_none(data.get('longitude')),
                    water_level=to_decimal_or_none(data.get('water_level')),
                    distance=to_decimal_or_none(data.get('distance')),
                    battery=to_decimal_or_none(data.get('battery')),
                    status=data.get('status'),
                    code=data.get('code'),
                    tank_status=data.get('tank_status'),
                    data_quality=data.get('data_quality', 'good')
                )
            
            if measurement:
                session.add(measurement)
                session.flush()  # Para obtener el ID
                
                # Verificar alertas
                self._check_alerts(session, sensor_type, data, measurement.id)
                
                session.commit()
                return measurement.id
            else:
                session.rollback()
                return None
                
        except Exception as e:
            logger.error(f"Error guardando en PostgreSQL: {e}", exc_info=True)
            if 'session' in locals():
                session.rollback()
            return None
        finally:
            if 'session' in locals():
                session.close()
    
    def save_to_redis(self, sensor_type, data):
        """Guardar datos en Redis para acceso rápido - VERSIÓN COMPLETAMENTE CORREGIDA"""
        try:
            device_name = data['device_name']
            
            # Función mejorada para convertir cualquier valor a string seguro para Redis
            def safe_str(value):
                if value is None:
                    return ''
                if isinstance(value, datetime):
                    return value.isoformat()
                if isinstance(value, (int, float, str, bool)):
                    return str(value)
                # Para cualquier otro tipo, intentamos convertirlo
                try:
                    return str(value)
                except:
                    return ''
            
            # **CORRECCIÓN CRÍTICA: Convertir TODOS los campos de data a strings seguros**
            safe_data = {k: safe_str(v) for k, v in data.items()}
            
            # Convertir timestamp a string si es datetime
            timestamp = data.get('timestamp')
            if isinstance(timestamp, datetime):
                timestamp_str = timestamp.isoformat()
            elif timestamp is None:
                timestamp_str = datetime.utcnow().isoformat()
            else:
                timestamp_str = safe_str(timestamp)
            
            # 1. Guardar última medición por dispositivo
            device_key = f"device:{device_name}"
            device_data = {
                'sensor_type': sensor_type,
                'last_update': timestamp_str,
                'latitude': safe_data.get('latitude', ''),
                'longitude': safe_data.get('longitude', ''),
                'status': 'online'
            }
            
            # Agregar datos específicos según tipo
            if sensor_type == 'aire':
                device_data.update({
                    'co2': safe_data.get('co2', ''),
                    'temperature': safe_data.get('temperature', ''),
                    'humidity': safe_data.get('humidity', ''),
                    'air_quality': safe_data.get('air_quality_category', '')
                })
            elif sensor_type == 'sonido':
                device_data.update({
                    'laeq': safe_data.get('laeq', ''),
                    'noise_category': safe_data.get('noise_category', '')
                })
            elif sensor_type == 'agua':
                device_data.update({
                    'water_level': safe_data.get('water_level', ''),
                    'tank_status': safe_data.get('tank_status', '')
                })
            
            # **CORRECCIÓN: Convertir todos los valores a bytes ANTES de enviar a Redis**
            device_data_bytes = {}
            for key, value in device_data.items():
                if isinstance(value, str):
                    device_data_bytes[key] = value.encode('utf-8')
                else:
                    device_data_bytes[key] = str(value).encode('utf-8')
            
            self.redis_client.hset(device_key, mapping=device_data_bytes)
            # self.redis_client.expire(device_key, 3600)
            self.redis_client.expire(device_key, 2592000)    # 30 días

            # 2. Guardar en lista de últimas mediciones
            history_key = f"history:{sensor_type}:{device_name}"
            history_data = {
                'timestamp': timestamp_str,
                'data': json.dumps(safe_data)  # Usamos safe_data que ya está convertido
            }
            
            self.redis_client.lpush(history_key, json.dumps(history_data))
            # self.redis_client.ltrim(history_key, 0, 49)
            self.redis_client.ltrim(history_key, 0, 9999)    # 10,000 registros
            
            # 3. Actualizar set de dispositivos activos
            active_key = f"active_devices:{sensor_type}"
            self.redis_client.sadd(active_key, device_name)
            # self.redis_client.expire(active_key, 7200)
            self.redis_client.expire(active_key, 2592000)    # 30 días
            
            # 4. Guardar para dashboard rápido (solo para aire)
            if sensor_type == 'aire' and data.get('co2'):
                co2_value = data.get('co2')
                if co2_value is not None:
                    try:
                        dashboard_key = "dashboard:air_quality"
                        self.redis_client.zadd(
                            dashboard_key,
                            {device_name: float(co2_value)}
                        )
                    except Exception as e:
                        logger.warning(f"Error guardando en dashboard: {e}")
            
            logger.info(f"🔍 Cacheado en Redis: {sensor_type} - {device_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error guardando en Redis: {e}", exc_info=True)
            # Log detallado para debugging
            logger.error(f"Datos que causaron el error: sensor_type={sensor_type}, device_name={data.get('device_name')}")
            logger.error(f"Tipos de datos: { {k: type(v) for k, v in data.items()} }")
            return False
    
    def _check_alerts(self, session, sensor_type, data, measurement_id):
        """Verificar y crear alertas si es necesario"""
        try:
            alerts_to_create = []
            device_name = data['device_name']
            
            # Alertas específicas por tipo de sensor
            if sensor_type == 'aire':
                # CO2 alto
                co2 = data.get('co2')
                if co2 and co2 > 1000:
                    severity = 'high' if co2 > 2000 else 'medium'
                    alerts_to_create.append({
                        'type': 'high_co2',
                        'message': f'Nivel de CO2 elevado: {co2:.1f} ppm',
                        'value': co2,
                        'threshold': 1000,
                        'severity': severity,
                        'air_measurement_id': measurement_id
                    })
                
                # Temperatura extrema
                temperature = data.get('temperature')
                if temperature:
                    if temperature > 30:
                        alerts_to_create.append({
                            'type': 'high_temperature',
                            'message': f'Temperatura alta: {temperature:.1f}°C',
                            'value': temperature,
                            'threshold': 30,
                            'severity': 'medium',
                            'air_measurement_id': measurement_id
                        })
                    elif temperature < 10:
                        alerts_to_create.append({
                            'type': 'low_temperature',
                            'message': f'Temperatura baja: {temperature:.1f}°C',
                            'value': temperature,
                            'threshold': 10,
                            'severity': 'medium',
                            'air_measurement_id': measurement_id
                        })
            
            elif sensor_type == 'sonido':
                # Ruido elevado
                laeq = data.get('laeq')
                if laeq and laeq > 75:
                    severity = 'high' if laeq > 85 else 'medium'
                    alerts_to_create.append({
                        'type': 'high_noise',
                        'message': f'Nivel de ruido elevado: {laeq:.1f} dB',
                        'value': laeq,
                        'threshold': 75,
                        'severity': severity,
                        'sound_measurement_id': measurement_id
                    })
            
            elif sensor_type == 'agua':
                # Nivel de agua bajo
                water_level = data.get('water_level')
                if water_level and water_level < 20:
                    severity = 'high' if water_level < 10 else 'medium'
                    alerts_to_create.append({
                        'type': 'low_water_level',
                        'message': f'Nivel de agua bajo: {water_level:.1f}%',
                        'value': water_level,
                        'threshold': 20,
                        'severity': severity,
                        'water_measurement_id': measurement_id
                    })
            
            # Alerta de batería baja (para todos los sensores)
            battery = data.get('battery')
            if battery and battery < 20:
                alerts_to_create.append({
                    'type': 'low_battery',
                    'message': f'Batería baja: {battery:.1f}%',
                    'value': battery,
                    'threshold': 20,
                    'severity': 'medium',
                    'air_measurement_id': measurement_id if sensor_type == 'aire' else None,
                    'sound_measurement_id': measurement_id if sensor_type == 'sonido' else None,
                    'water_measurement_id': measurement_id if sensor_type == 'agua' else None
                })
            
            # Crear alertas en base de datos
            for alert_data in alerts_to_create:
                alert = Alert(
                    device_name=device_name,
                    sensor_type=sensor_type,
                    alert_type=alert_data['type'],
                    message=alert_data['message'],
                    value=alert_data['value'],
                    threshold=alert_data['threshold'],
                    severity=alert_data['severity'],
                    air_measurement_id=alert_data.get('air_measurement_id'),
                    sound_measurement_id=alert_data.get('sound_measurement_id'),
                    water_measurement_id=alert_data.get('water_measurement_id')
                )
                session.add(alert)
                
                # También guardar en Redis para alertas en tiempo real
                alert_key = f"alert:{device_name}:{alert_data['type']}:latest"
                alert_cache = {
                    'message': alert_data['message'],
                    'severity': alert_data['severity'],
                    'timestamp': datetime.utcnow().isoformat(),
                    'value': str(alert_data['value'])
                }
                
                # Convertir a bytes para Redis
                alert_cache_bytes = {}
                for k, v in alert_cache.items():
                    if isinstance(v, str):
                        alert_cache_bytes[k] = v.encode('utf-8')
                    else:
                        alert_cache_bytes[k] = str(v).encode('utf-8')
                
                try:
                    self.redis_client.hset(alert_key, mapping=alert_cache_bytes)
                    self.redis_client.expire(alert_key, 3600)  # Expira en 1 hora
                except Exception as e:
                    logger.warning(f"Error guardando alerta en Redis: {e}")
                
                logger.warning(f"⚠️ Alerta creada: {alert_data['type']} - {device_name}")
            
            return len(alerts_to_create) > 0
            
        except Exception as e:
            logger.error(f"Error verificando alertas: {e}", exc_info=True)
            return False
    
    def get_device_status(self, device_name):
        """Obtener estado de un dispositivo desde Redis"""
        try:
            device_key = f"device:{device_name}"
            data = self.redis_client.hgetall(device_key)
            # Decodificar bytes a string
            return {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()} if data else {}
        except:
            return {}
    
    def close(self):
        """Cerrar conexiones de manera segura"""
        try:
            if self.postgres_engine:
                self.postgres_engine.dispose()
                logger.info("🔌 Conexión PostgreSQL cerrada")
        except Exception as e:
            logger.error(f"Error cerrando PostgreSQL: {e}")
        
        try:
            if self.redis_client:
                self.redis_client.close()
                logger.info("🔌 Conexión Redis cerrada")
        except Exception as e:
            logger.error(f"Error cerrando Redis: {e}")