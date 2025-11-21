# Añadir junto con los otros imports
from api_sender import SensorDataAPISender
import json
import logging
from kafka import KafkaConsumer
from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np
from datetime import datetime
from config import KafkaConfig, ETLConfig, APIConfig
from schemas import AirQualityData, SoundData, WaterData
from validation import DataValidator
from data_warehouse import DataWarehouse
from writers.firestore_writer import FirestoreWriter
from writers.supabase_writer import SupabaseWriter
import os

# Garantizar permisos en runtime
os.makedirs("/app/data_warehouse", exist_ok=True)
os.makedirs("/app/processed_data", exist_ok=True)

try:
    os.chmod("/app/data_warehouse", 0o777)
    os.chmod("/app/processed_data", 0o777)
except:
    pass

LOG_DIR = "/tmp/etl_logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        #logging.FileHandler(f"{LOG_DIR}/etl_processor.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class SensorDataETL:
    def __init__(self):
        try:
            # ✅ CORREGIDO: Añadir value_deserializer para convertir bytes a JSON
            self.consumer = KafkaConsumer(
                KafkaConfig.TOPIC_AIR_QUALITY,
                KafkaConfig.TOPIC_SOUND,
                KafkaConfig.TOPIC_WATER,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),  # ✅ DESERIALIZADOR AÑADIDO
                **KafkaConfig.CONSUMER_CONFIG
            )
            self.validator = DataValidator()
            self.warehouse = DataWarehouse()
            
            # Crear directorios
            os.makedirs(ETLConfig.OUTPUT_DIR, exist_ok=True)
            os.makedirs(ETLConfig.LOG_DIR, exist_ok=True)
            
            # Métricas
            self.metrics = {
                'total_received': 0,
                'successfully_processed': 0,
                'processing_errors': 0,
                'validation_errors': 0,
                'last_processed': None,
                'start_time': datetime.now()
            }
            
            # Buffer para procesamiento por lotes
            self.batch_buffer = {
                'air_quality': [],
                'sound': [],
                'water': []
            }
            
            # ✅ INICIALIZAR API Sender
            self.api_sender = SensorDataAPISender(
                base_url=APIConfig.BASE_URL,
                api_key=APIConfig.API_KEY,
                timeout=APIConfig.TIMEOUT
            )
            
            logger.info("✅ Consumidor ETL inicializado correctamente")
            
            # Verificar conexión con el backend
            if APIConfig.ENABLED:
                if self.api_sender.health_check():
                    logger.info("✅ Backend disponible")
                else:
                    logger.warning("⚠️ Backend no disponible, los datos se guardarán solo localmente")
            
        except Exception as e:
            logger.error(f"❌ Error inicializando consumidor ETL: {e}")
            raise


    def extract_signal_metrics(self, data: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """Extrae y calcula métricas de señal"""
        rssi_values = []
        snr_values = []
        
        for i in range(4):
            # RSSI
            rssi_key = f'rxInfo[{i}].rssi'
            rssi_val = data.get(rssi_key)
            if rssi_val is not None and rssi_val != '':
                try:
                    rssi_float = float(rssi_val)
                    if ETLConfig.MIN_RSSI <= rssi_float <= ETLConfig.MAX_RSSI:
                        rssi_values.append(rssi_float)
                except (ValueError, TypeError):
                    pass
            
            # SNR
            snr_key = f'rxInfo[{i}].snr'
            snr_val = data.get(snr_key)
            if snr_val is not None and snr_val != '':
                try:
                    snr_float = float(snr_val)
                    if ETLConfig.MIN_SNR <= snr_float <= ETLConfig.MAX_SNR:
                        snr_values.append(snr_float)
                except (ValueError, TypeError):
                    pass
        
        return {
            'rssi_avg': np.mean(rssi_values) if rssi_values else None,
            'snr_avg': np.mean(snr_values) if snr_values else None,
            'rssi_min': min(rssi_values) if rssi_values else None,
            'rssi_max': max(rssi_values) if rssi_values else None,
            'gateway_count': len(rssi_values)
        }
    
    def extract_location(self, location_str: str) -> tuple:
        """Extrae coordenadas de ubicación"""
        if not location_str or not isinstance(location_str, str) or ',' not in location_str:
            return None, None
        
        try:
            # Limpiar y dividir la cadena
            clean_str = location_str.strip().strip('"[]')
            parts = clean_str.split(',')
            
            if len(parts) >= 2:
                lat = float(parts[0].strip())
                lon = float(parts[1].strip())
                
                # Validar rangos de coordenadas
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    return lat, lon
        
        except (ValueError, AttributeError, IndexError) as e:
            logger.debug(f"Error parseando ubicación '{location_str}': {e}")
        
        return None, None
    
    def safe_float(self, value) -> Optional[float]:
        """Conversión segura a float"""
        if value is None or value == '':
            return None
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def safe_int(self, value) -> Optional[int]:
        """Conversión segura a int"""
        if value is None or value == '':
            return None
        
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def transform_to_schema(self, data: Dict[str, Any], sensor_type: str):
        """
        TRANSFORMACIÓN ETL MEJORADA - CORAZÓN DEL PROCESAMIENTO
        ✅ Manejo robusto de errores
        ✅ Validación de calidad de datos  
        ✅ Límites de campos nulos
        ✅ Detección de fallas en sensores
        """
        try:
            # ===========================================================================
            # 🎯 1. VALIDACIÓN DE CAMPOS CRÍTICOS
            # ===========================================================================
            critical_fields = ['_id', 'time', 'deviceInfo.deviceName']
            missing_critical = [field for field in critical_fields if not data.get(field)]
            
            if missing_critical:
                logger.warning(f"🚨 Campos críticos faltantes: {missing_critical} - ID: {data.get('_id', 'Unknown')}")
                return None

            # ===========================================================================
            # 🎯 2. EXTRACCIÓN CON VALIDACIÓN MEJORADA
            # ===========================================================================
            
            # ✅ Métricas de señal con validación
            signal_metrics = self.extract_signal_metrics(data)
            
            # ✅ Ubicación con fallback
            location_str = data.get('deviceInfo.tags.Location', '')
            latitude, longitude = self.extract_location(location_str)
            
            # ✅ Timestamp con manejo robusto
            timestamp = self.safe_timestamp(data.get('time'))
            if not timestamp:
                logger.warning(f"🚨 Timestamp inválido: {data.get('time')} - ID: {data.get('_id')}")
                return None

            # ===========================================================================
            # 🎯 3. DATOS BASE CON VALIDACIÓN DE CALIDAD
            # ===========================================================================
            base_data = {
                '_id': self.safe_string(data.get('_id')),
                'devAddr': self.safe_string(data.get('devAddr')),
                'deduplicationId': self.safe_string(data.get('deduplicationId')),
                'time': timestamp,
                'device_class': self.safe_string(data.get('deviceInfo.deviceClassEnabled')),
                'tenant_name': self.safe_string(data.get('deviceInfo.tenantName')),
                'device_name': self.safe_string(data.get('deviceInfo.deviceName')),
                'location': self.safe_string(data.get('deviceInfo.tags.Address')),
                'latitude': latitude,
                'longitude': longitude,
                'battery_level': self.safe_float(data.get('batteryLevel')),
                'fCnt': self.safe_int(data.get('fCnt')),
                'rssi': signal_metrics['rssi_avg'],
                'snr': signal_metrics['snr_avg']
            }

            # ===========================================================================
            # 🎯 4. VALIDACIÓN DE CALIDAD - MÁXIMO DE CAMPOS NULOS
            # ===========================================================================
            null_count = sum(1 for value in base_data.values() if value is None)
            total_fields = len(base_data)
            null_percentage = (null_count / total_fields) * 100
            
            # 🚨 RECHAZAR si más del 60% de campos base son nulos
            if null_percentage > 60:
                logger.warning(f"🚨 Demasiados campos nulos ({null_percentage:.1f}%) - ID: {data.get('_id')}")
                return None

            # ===========================================================================
            # 🎯 5. TRANSFORMACIÓN ESPECÍFICA POR SENSOR
            # ===========================================================================
            if sensor_type == 'air_quality':
                sensor_specific = {
                    'co2': self.safe_float(data.get('object.co2')),
                    'temperature': self.safe_float(data.get('object.temperature')),
                    'humidity': self.safe_float(data.get('object.humidity')),
                    'pressure': self.safe_float(data.get('object.pressure')),
                    'co2_status': self.safe_string(data.get('object.co2_status')),
                    'temperature_status': self.safe_string(data.get('object.temperature_status')),
                    'humidity_status': self.safe_string(data.get('object.humidity_status')),
                    'pressure_status': self.safe_string(data.get('object.pressure_status'))
                }
                
                # ✅ Validación específica para calidad de aire
                if not self.validate_air_quality_data(sensor_specific):
                    return None
                    
                return AirQualityData(**base_data, **sensor_specific)
                
            elif sensor_type == 'sound':
                sensor_specific = {
                    'laeq': self.safe_float(data.get('object.LAeq')),
                    'lai': self.safe_float(data.get('object.LAI')),
                    'laimax': self.safe_float(data.get('object.LAImax')),
                    'sound_status': self.safe_string(data.get('object.status'))
                }
                
                # ✅ Validación específica para sonido
                if not self.validate_sound_data(sensor_specific):
                    return None
                    
                return SoundData(**base_data, **sensor_specific)
                
            elif sensor_type == 'water':
                sensor_specific = {
                    'distance': self.safe_float(data.get('object.distance')),
                    'position': self.safe_string(data.get('object.position')),
                    'water_status': self.safe_string(data.get('object.status'))
                }
                
                # ✅ Validación específica para agua
                if not self.validate_water_data(sensor_specific):
                    return None
                    
                return WaterData(**base_data, **sensor_specific)

            return None
            
        except Exception as e:
            logger.error(f"❌ Error crítico en transformación ETL: {e} - Data: {data.get('_id', 'Unknown')}")
            return None

    # ===========================================================================
    # 🛡️ FUNCIONES DE VALIDACIÓN MEJORADAS
    # ===========================================================================

    def safe_timestamp(self, time_str) -> Optional[datetime]:
        """Conversión segura de timestamp con múltiples formatos"""
        if not time_str:
            return None
        
        try:
            # Manejar diferentes formatos de timestamp
            if isinstance(time_str, datetime):
                return time_str
                
            time_str = str(time_str).strip()
            
            # Formato ISO con Z
            if 'Z' in time_str:
                return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            
            # Formato ISO sin Z
            elif 'T' in time_str:
                return datetime.fromisoformat(time_str)
            
            # Otros formatos comunes
            else:
                # Intentar parsear con varios formatos
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']:
                    try:
                        return datetime.strptime(time_str, fmt)
                    except ValueError:
                        continue
            
            logger.warning(f"⚠️ Formato de timestamp no reconocido: {time_str}")
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Error parseando timestamp '{time_str}': {e}")
            return None

    def safe_string(self, value) -> Optional[str]:
        """Conversión segura a string con limpieza"""
        if value is None:
            return None
        
        try:
            cleaned = str(value).strip()
            return cleaned if cleaned else None
        except Exception:
            return None

    def validate_air_quality_data(self, data: Dict) -> bool:
        """Valida datos de calidad de aire"""
        # ✅ Verificar que tenga al menos un campo de medición válido
        measurement_fields = ['co2', 'temperature', 'humidity', 'pressure']
        has_valid_measurement = any(
            data.get(field) is not None 
            for field in measurement_fields
        )
        
        if not has_valid_measurement:
            logger.warning("🚨 Calidad de aire: Sin mediciones válidas")
            return False
        
        # ✅ Validar rangos razonables
        if data.get('co2') is not None and not (300 <= data['co2'] <= 5000):
            logger.warning(f"🚨 CO2 fuera de rango: {data['co2']}")
            return False
            
        if data.get('temperature') is not None and not (-50 <= data['temperature'] <= 60):
            logger.warning(f"🚨 Temperatura fuera de rango: {data['temperature']}")
            return False
            
        return True

    def validate_sound_data(self, data: Dict) -> bool:
        """Valida datos de sonido"""
        if data.get('laeq') is None:
            logger.warning("🚨 Sonido: LAeq es requerido")
            return False
        
        # ✅ Validar rangos de sonido
        if data['laeq'] is not None and not (30 <= data['laeq'] <= 120):
            logger.warning(f"🚨 LAeq fuera de rango: {data['laeq']}")
            return False
            
        return True

    def validate_water_data(self, data: Dict) -> bool:
        """Valida datos de agua"""
        # ✅ Debe tener distancia O posición
        if data.get('distance') is None and data.get('position') is None:
            logger.warning("🚨 Agua: Sin distancia ni posición")
            return False
        
        # ✅ Validar rango de distancia
        if data.get('distance') is not None and not (0 <= data['distance'] <= 100):
            logger.warning(f"🚨 Distancia fuera de rango: {data['distance']}")
            return False
            
        return True
    

    def process_message(self, message) -> bool:
        """Procesa un mensaje individual"""
        self.metrics['total_received'] += 1
        
        try:
            data = message.value
            topic = message.topic
            
            # Determinar tipo de sensor
            topic_to_type = {
                KafkaConfig.TOPIC_AIR_QUALITY: 'air_quality',
                KafkaConfig.TOPIC_SOUND: 'sound',
                KafkaConfig.TOPIC_WATER: 'water'
            }
            
            sensor_type = topic_to_type.get(topic)
            if not sensor_type:
                logger.warning(f"⚠️ Topic desconocido: {topic}")
                return False
            
            # Validar el mensaje
            is_valid, reason = self.validator.validate_sensor_record(data, sensor_type)
            if not is_valid:
                self.metrics['validation_errors'] += 1
                logger.debug(f"🚫 Mensaje inválido en consumidor: {reason}")
                return False
            
            # Transformar datos
            transformed_data = self.transform_to_schema(data, sensor_type)
            if not transformed_data:
                self.metrics['processing_errors'] += 1
                return False
            
            # ============================================
            # 📌 GUARDAR EN FIREBASE Y SUPABASE
            # ============================================
            record_dict = vars(transformed_data)

            # Guardar en Firestore (real time)
            FirestoreWriter.save_reading(sensor_type, record_dict)

            # Guardar en Supabase/Postgres (histórico)
            SupabaseWriter.save_sensor_record(sensor_type, record_dict)

            # Agregar al data warehouse
            self.warehouse.add_sensor_data(sensor_type, transformed_data)
            
            # Agregar al buffer por lotes
            self.batch_buffer[sensor_type].append(transformed_data)
            
            # Procesar lote si está lleno
            if len(self.batch_buffer[sensor_type]) >= ETLConfig.BATCH_SIZE:
                self.process_batch(sensor_type)
            
            self.metrics['successfully_processed'] += 1
            self.metrics['last_processed'] = datetime.now()
            
            # Log periódico
            if self.metrics['successfully_processed'] % 100 == 0:
                self.log_metrics()
            
            return True
            
        except Exception as e:
            self.metrics['processing_errors'] += 1
            logger.error(f"❌ Error procesando mensaje: {e}")
            return False
    
    def process_batch(self, sensor_type: str):
        """Procesa un lote de datos"""
        batch = self.batch_buffer[sensor_type]
        if not batch:
            return
        
        try:
            # Convertir a DataFrame
            records = [vars(item) for item in batch]
            df = pd.DataFrame(records)
            
            logger.info(f"📦 Procesando lote de {len(batch)} registros de {sensor_type}")
            
            # Guardar en data warehouse (simulado)
            self.save_to_data_warehouse(df, sensor_type)
            
            # Limpiar buffer
            self.batch_buffer[sensor_type] = []
            
        except Exception as e:
            logger.error(f"❌ Error procesando lote de {sensor_type}: {e}")
    
    def save_to_data_warehouse(self, df: pd.DataFrame, sensor_type: str):
        """Guarda datos procesados (simulación)"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{ETLConfig.OUTPUT_DIR}/{sensor_type}_batch_{timestamp}.parquet"
            
            #df.to_parquet(filename, index=False)
            logger.debug(f"💾 Lote guardado: {filename}")
            
        except Exception as e:
            logger.error(f"❌ Error guardando lote: {e}")
    
    def start_processing(self):
        """Inicia el procesamiento continuo"""
        logger.info("🔄 Iniciando procesamiento ETL en tiempo real...")
        
        try:
            for message in self.consumer:
                success = self.process_message(message)
                
                if success:
                    self.consumer.commit()
                
                # Procesar lotes pendientes periódicamente
                for sensor_type in self.batch_buffer:
                    if self.batch_buffer[sensor_type]:
                        self.process_batch(sensor_type)
                
                # Exportar reportes cada 500 mensajes
                if self.metrics['total_received'] % 500 == 0:
                    self.export_warehouse_reports()
                
        except KeyboardInterrupt:
            logger.info("🛑 Deteniendo procesamiento ETL...")
        except Exception as e:
            logger.error(f"❌ Error crítico en el consumidor: {e}")
        finally:
            # Procesar lotes pendientes antes de cerrar
            for sensor_type in self.batch_buffer:
                if self.batch_buffer[sensor_type]:
                    self.process_batch(sensor_type)
            
            # Exportar reportes finales
            self.export_final_reports()
            
            self.consumer.close()
            self.log_final_metrics()
    
        def export_warehouse_reports(self):
            """Exporta reportes al backend API en lugar de archivos JSON"""
            try:
                if not APIConfig.ENABLED:
                    logger.info("🚫 Envío a API deshabilitado en configuración")
                    return
                
                # ✅ CORREGIDO: Verificar si el warehouse tiene los atributos esperados
                if not hasattr(self.warehouse, 'air_quality_data') or not hasattr(self.warehouse, 'sound_data') or not hasattr(self.warehouse, 'water_data'):
                    logger.error("❌ El DataWarehouse no tiene la estructura esperada")
                    # Fallback a JSON local
                    self._fallback_to_local_json()
                    return
                
                # Obtener todos los datos del warehouse
                all_data = {
                    'air_quality': self.warehouse.air_quality_data,
                    'sound': self.warehouse.sound_data, 
                    'water': self.warehouse.water_data
                }
                
                # Verificar que hay datos para enviar
                total_records = sum(len(records) for records in all_data.values())
                if total_records == 0:
                    logger.info("📭 No hay datos nuevos para enviar al backend")
                    return
                
                logger.info(f"📤 Preparando envío de {total_records} registros al backend...")
                
                # Intentar enviar datos unificados
                success = self.api_sender.send_unified_sensor_data(all_data)
                
                if success:
                    logger.info("✅ Todos los datos enviados exitosamente al backend")
                else:
                    logger.error("❌ Falló el envío al backend, los datos se mantienen localmente")
                    self._fallback_to_local_json()
                
            except Exception as e:
                logger.error(f"❌ Error exportando reportes al backend: {e}")
                self._fallback_to_local_json()
    
    def _fallback_to_local_json(self):
        """Fallback para guardar datos localmente cuando falla el API"""
        try:
            # ✅ AÑADIR: Verificar que el warehouse tenga los métodos necesarios
            if hasattr(self.warehouse, 'export_individual_sensor_json') and hasattr(self.warehouse, 'export_to_json'):
                individual_files = self.warehouse.export_individual_sensor_json()
                consolidated_file = self.warehouse.export_to_json()
                logger.info(f"💾 Fallback: Datos guardados localmente en {len(individual_files)} archivos")
            else:
                logger.error("❌ El DataWarehouse no tiene métodos de exportación JSON")
        except Exception as fallback_error:
            logger.error(f"❌ Error en fallback local: {fallback_error}")

    def export_final_reports(self):
        """Exporta reportes finales al backend"""
        try:
            logger.info("📋 Enviando reportes finales al backend...")
            
            if not APIConfig.ENABLED:
                logger.info("🚫 Envío a API deshabilitado, generando solo reportes locales")
                # Generar reportes locales como fallback
                if hasattr(self.warehouse, 'export_to_json') and hasattr(self.warehouse, 'export_individual_sensor_json'):
                    final_file = self.warehouse.export_to_json("data_warehouse_final_report.json")
                    individual_files = self.warehouse.export_individual_sensor_json()
                return
            
            # Obtener todos los datos para envío final
            all_data = {
                'air_quality': self.warehouse.air_quality_data,
                'sound': self.warehouse.sound_data,
                'water': self.warehouse.water_data
            }
            
            total_records = sum(len(records) for records in all_data.values())
            
            if total_records > 0:
                # Enviar datos unificados
                success = self.api_sender.send_unified_sensor_data(all_data)
                
                if success:
                    logger.info(f"✅ Reporte final enviado: {total_records} registros")
                else:
                    logger.error("❌ Falló el envío del reporte final")
            else:
                logger.info("📭 No hay datos para el reporte final")
            
            # Generar reporte local como backup siempre
            try:
                if hasattr(self.warehouse, 'generate_warehouse_report'):
                    warehouse_report = self.warehouse.generate_warehouse_report()
                
                if hasattr(self.warehouse, 'export_to_json'):
                    final_file = self.warehouse.export_to_json("data_warehouse_final_report.json")
                    logger.info(f"💾 Backup local guardado: {final_file}")
                
                # Mostrar resumen ejecutivo
                if 'warehouse_report' in locals():
                    self.print_executive_summary(warehouse_report)
                
            except Exception as local_error:
                logger.error(f"❌ Error generando backup local: {local_error}")
                
        except Exception as e:
            logger.error(f"❌ Error en exportación final: {e}")
    

    def print_executive_summary(self, report: Dict[str, Any]):
        """Muestra un resumen ejecutivo del procesamiento"""
        logger.info("=" * 70)
        logger.info("📊 RESUMEN EJECUTIVO - DATA WAREHOUSE")
        logger.info("=" * 70)
        
        # Resumen por sensor
        for sensor_type, summary in report.get('summary', {}).items():
            logger.info(f"🔹 {sensor_type.upper()}: {summary.get('total_records', 0)} registros")
        
        # Calidad de datos
        quality = report.get('data_quality', {})
        for sensor_type, metrics in quality.items():
            validity = metrics.get('validity_rate', 0)
            status = "🟢 EXCELENTE" if validity > 0.9 else "🟡 ACEPTABLE" if validity > 0.7 else "🔴 CRÍTICO"
            logger.info(f"📈 Calidad {sensor_type}: {validity:.1%} - {status}")
        
        # Recomendaciones
        recommendations = report.get('recommendations', [])
        if recommendations:
            logger.info("💡 RECOMENDACIONES:")
            for rec in recommendations[:5]:
                logger.info(f"   • {rec}")
    
    def log_metrics(self):
        """Log de métricas periódicas"""
        success_rate = (
            self.metrics['successfully_processed'] / self.metrics['total_received'] 
            if self.metrics['total_received'] > 0 else 0
        )
        
        runtime = datetime.now() - self.metrics['start_time']
        
        logger.info(
            f"📈 MÉTRICAS - "
            f"Runtime: {runtime}, "
            f"Recibidos: {self.metrics['total_received']}, "
            f"Procesados: {self.metrics['successfully_processed']}, "
            f"Errores: {self.metrics['processing_errors']}, "
            f"Válidos: {self.metrics['validation_errors']}, "
            f"Tasa éxito: {success_rate:.2%}"
        )
    
    def log_final_metrics(self):
        """Reporte final completo"""
        logger.info("=" * 60)
        logger.info("📊 REPORTE FINAL DE PROCESAMIENTO ETL")
        logger.info("=" * 60)
        
        self.log_metrics()
        
        # Reporte de validación
        validation_report = self.validator.get_validation_report()
        logger.info("🔍 REPORTE DE VALIDACIÓN:")
        logger.info(f"   Registros válidos: {validation_report['valid_records']}")
        logger.info(f"   Registros vacíos: {validation_report['empty_records']}")
        logger.info(f"   Registros inválidos: {validation_report['invalid_records']}")
        logger.info(f"   Tasa de validez: {validation_report['validity_rate']:.2%}")
        logger.info(f"   Razones de rechazo: {validation_report['rejection_reasons']}")

if __name__ == "__main__":
    try:
        etl_processor = SensorDataETL()
        etl_processor.start_processing()
    except Exception as e:
        logger.error(f"❌ No se pudo iniciar el procesador ETL: {e}")