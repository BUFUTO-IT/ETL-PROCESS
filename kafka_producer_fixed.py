import json
import time
import pandas as pd
import numpy as np  # ✅ CORREGIDO: Importación agregada
from kafka import KafkaProducer
from config import KafkaConfig
from validation import DataValidator
import logging
from typing import Dict, Any
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SensorDataProducer:
    def __init__(self):
        try:
            self.producer = KafkaProducer(
                **KafkaConfig.PRODUCER_CONFIG,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
            )
            self.validator = DataValidator()
            logger.info("✅ Productor de Kafka inicializado correctamente")
        except Exception as e:
            logger.error(f"❌ Error inicializando productor: {e}")
            raise
    
    def is_empty_row(self, row: pd.Series) -> bool:
        """Verifica si una fila está completamente vacía (solo comas)"""
        return row.isna().all() or row.astype(str).str.strip().eq('').all()
    
    def detect_sensor_type(self, record: Dict[str, Any]) -> str:
        """Detecta automáticamente el tipo de sensor basado en los datos"""
        device_name = str(record.get('deviceInfo.deviceName', '')).lower()
        
        # Detección por campos específicos
        if record.get('object.co2') is not None and record.get('object.co2') != '':
            return 'air_quality'
        elif record.get('object.LAeq') is not None and record.get('object.LAeq') != '':
            return 'sound'
        elif record.get('object.distance') is not None and record.get('object.distance') != '':
            return 'water'
        
        # Detección por nombre de dispositivo
        if any(keyword in device_name for keyword in ['co2', 'ems', 'air']):
            return 'air_quality'
        elif any(keyword in device_name for keyword in ['sls', 'sound', 'noise']):
            return 'sound'
        elif any(keyword in device_name for keyword in ['uds', 'water', 'level']):
            return 'water'
        
        return 'unknown'
    
    def preprocess_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Limpia y preprocesa el registro"""
        processed = {}
        
        for key, value in record.items():
            # Convertir tipos de pandas a Python nativo
            if pd.isna(value) or value is None or value == '':
                processed[key] = None
            elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
                processed[key] = str(value)
            elif isinstance(value, (np.int64, np.float64)):
                if pd.isna(value):
                    processed[key] = None
                else:
                    processed[key] = float(value) if np.isnan(value) else int(value) if value == int(value) else float(value)
            else:
                processed[key] = value
        
        return processed
    
    def send_sensor_data(self, topic: str, data: Dict[str, Any]) -> bool:
        """Envía datos al topic de Kafka con manejo de errores"""
        try:
            future = self.producer.send(topic, value=data)
            future.get(timeout=10)
            logger.debug(f"📤 Enviado a {topic}: {data.get('_id', 'Unknown')}")
            return True
        except Exception as e:
            logger.error(f"❌ Error enviando a {topic}: {e}")
            return False
    
    def analyze_csv_quality(self, csv_file: str) -> Dict[str, Any]:
        """Analiza la calidad del archivo CSV antes de procesar"""
        try:
            df = pd.read_csv(csv_file, low_memory=False)
            
            # Análisis de calidad
            total_rows = len(df)
            empty_rows = sum(1 for _, row in df.iterrows() if self.is_empty_row(row))
            
            # Análisis por columna
            column_analysis = {}
            for column in df.columns:
                null_count = df[column].isna().sum()
                empty_count = (df[column].astype(str).str.strip() == '').sum()
                unique_count = df[column].nunique()
                
                column_analysis[column] = {
                    'null_count': int(null_count),
                    'empty_count': int(empty_count),
                    'unique_values': int(unique_count),
                    'null_percentage': float((null_count / total_rows) * 100),
                    'sample_data': df[column].head(3).tolist() if unique_count > 0 else []
                }
            
            return {
                'file_name': csv_file,
                'total_rows': total_rows,
                'empty_rows': empty_rows,
                'empty_percentage': (empty_rows / total_rows) * 100,
                'columns_count': len(df.columns),
                'column_analysis': column_analysis,
                'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()}
            }
            
        except Exception as e:
            logger.error(f"❌ Error analizando calidad de {csv_file}: {e}")
            return {}
    
    def stream_csv_data(self, csv_file: str, delay: float = 0.1):
        """Transmite datos desde CSV con validación robusta"""
        if not os.path.exists(csv_file):
            logger.error(f"❌ Archivo no encontrado: {csv_file}")
            return
        
        try:
            logger.info(f"📖 Cargando datos desde: {csv_file}")
            
            # Análisis de calidad primero
            quality_report = self.analyze_csv_quality(csv_file)
            logger.info(f"📊 REPORTE DE CALIDAD - {csv_file}:")
            logger.info(f"   📋 Total filas: {quality_report.get('total_rows', 0)}")
            logger.info(f"   🚫 Filas vacías: {quality_report.get('empty_rows', 0)}")
            logger.info(f"   📈 Porcentaje vacío: {quality_report.get('empty_percentage', 0):.2f}%")
            
            # Leer CSV
            df = pd.read_csv(csv_file, low_memory=False)
            logger.info(f"📊 Encontrados {len(df)} registros en {csv_file}")
            
            stats = {
                'processed': 0,
                'sent': 0,
                'empty': 0,
                'invalid': 0,
                'errors': 0
            }
            
            for index, row in df.iterrows():
                try:
                    # Saltar filas completamente vacías
                    if self.is_empty_row(row):
                        stats['empty'] += 1
                        continue
                    
                    # Convertir a diccionario y limpiar
                    raw_record = row.where(pd.notnull(row), None).to_dict()
                    record = self.preprocess_record(raw_record)
                    
                    # Detectar tipo de sensor
                    sensor_type = self.detect_sensor_type(record)
                    
                    if sensor_type == 'unknown':
                        stats['invalid'] += 1
                        if stats['invalid'] <= 3:
                            logger.warning(f"🔍 Tipo de sensor desconocido: {record.get('_id')}")
                        continue
                    
                    # Validar registro
                    is_valid, reason = self.validator.validate_sensor_record(record, sensor_type)
                    
                    if is_valid:
                        topic_map = {
                            'air_quality': KafkaConfig.TOPIC_AIR_QUALITY,
                            'sound': KafkaConfig.TOPIC_SOUND,
                            'water': KafkaConfig.TOPIC_WATER
                        }
                        topic = topic_map[sensor_type]
                        
                        if self.send_sensor_data(topic, record):
                            stats['sent'] += 1
                        else:
                            stats['errors'] += 1
                    else:
                        stats['invalid'] += 1
                    
                    stats['processed'] += 1
                    
                    # Simular tiempo real
                    time.sleep(delay)
                    
                    # Log de progreso
                    if stats['processed'] % 50 == 0:
                        logger.info(
                            f"📈 Progreso {csv_file}: "
                            f"Procesados: {stats['processed']}/{len(df)}, "
                            f"Enviados: {stats['sent']}, "
                            f"Vacíos: {stats['empty']}, "
                            f"Inválidos: {stats['invalid']}"
                        )
                        
                except Exception as e:
                    stats['errors'] += 1
                    logger.error(f"❌ Error procesando registro {index}: {e}")
                    continue
            
            # Reporte final
            logger.info(f"✅ Transmisión completada: {csv_file}")
            logger.info(f"   📋 Total procesados: {stats['processed']}")
            logger.info(f"   📤 Enviados exitosamente: {stats['sent']}")
            logger.info(f"   🚫 Filas vacías: {stats['empty']}")
            logger.info(f"   ⚠️ Registros inválidos: {stats['invalid']}")
            logger.info(f"   ❌ Errores de envío: {stats['errors']}")
            
            # Reporte de validación
            validation_report = self.validator.get_validation_report()
            logger.info("📊 REPORTE DE VALIDACIÓN:")
            logger.info(f"   Registros válidos: {validation_report['valid_records']}")
            logger.info(f"   Registros vacíos: {validation_report['empty_records']}")
            logger.info(f"   Tasa de validez: {validation_report['validity_rate']:.2%}")
            
        except Exception as e:
            logger.error(f"❌ Error crítico procesando {csv_file}: {e}")
    
    def close(self):
        """Cierra el productor de manera segura"""
        try:
            self.producer.flush(timeout=10)
            self.producer.close()
            logger.info("🔚 Productor de Kafka cerrado correctamente")
        except Exception as e:
            logger.error(f"❌ Error cerrando productor: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Productor de datos de sensores')
    parser.add_argument('--delay', type=float, default=0.05, help='Delay entre mensajes')
    parser.add_argument('--files', nargs='+', help='Archivos CSV a procesar')
    
    args = parser.parse_args()
    
    producer = SensorDataProducer()
    
    try:
        default_files = ['co2.csv', 'sound.csv', 'water.csv']
        files_to_process = args.files if args.files else default_files
        
        logger.info("🚀 Iniciando transmisión de datos de sensores...")
        
        for csv_file in files_to_process:
            if os.path.exists(csv_file):
                producer.stream_csv_data(csv_file, args.delay)
            else:
                logger.warning(f"⚠️ Archivo no encontrado: {csv_file}")
        
        logger.info("🎉 Transmisión de todos los archivos completada")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Transmisión interrumpida por el usuario")
    except Exception as e:
        logger.error(f"❌ Error en la transmisión: {e}")
    finally:
        producer.close()