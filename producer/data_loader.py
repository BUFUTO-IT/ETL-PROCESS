# producer/data_loader.py
import pandas as pd
import numpy as np
from datetime import datetime
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    """Cargador de datos para los datasets de sensores"""
    
    def __init__(self, dataset_path='datasets'):
        self.dataset_path = dataset_path
        self.processed_indices = set()  # Seguimiento de datos enviados

    def get_batch_ordered(self, sensor_type, batch_size=50, start_index=0):
        """Obtener lote ORDENADO por timestamp"""
        all_data = self.load_and_process_data(sensor_type)
        
        if not all_data:
            return []
        
        # Ordenar por timestamp
        all_data.sort(key=lambda x: x.get('timestamp', ''))
        
        # Obtener lote secuencial (no aleatorio)
        end_index = min(start_index + batch_size, len(all_data))
        batch = all_data[start_index:end_index]
        
        return batch, end_index
    
    def get_all_data_ordered(self, sensor_type):
        """Obtener TODOS los datos ordenados"""
        all_data = self.load_and_process_data(sensor_type)
        
        if not all_data:
            return []
        
        # Ordenar por timestamp
        return sorted(all_data, key=lambda x: x.get('timestamp', ''))
        
    def load_and_process_data(self, sensor_type):
        """Cargar y procesar datos según tipo de sensor"""
        filepath = f"{self.dataset_path}/{sensor_type}.csv"
        
        try:
            logger.info(f"Cargando datos de {sensor_type} desde {filepath}")
            
            if sensor_type == 'aire':
                return self._process_air_data(filepath)
            elif sensor_type == 'sonido':
                return self._process_sound_data(filepath)
            elif sensor_type == 'agua':
                return self._process_water_data(filepath)
            else:
                logger.error(f"Tipo de sensor no soportado: {sensor_type}")
                return []
                
        except FileNotFoundError:
            logger.error(f"Archivo no encontrado: {filepath}")
            return []
        except Exception as e:
            logger.error(f"Error cargando datos de {sensor_type}: {e}")
            return []
    
    def _process_air_data(self, filepath):
        """Procesar datos de aire"""
        df = pd.read_csv(filepath, low_memory=False)
        

        # AÑADIR: Verificar que el DataFrame no esté vacío
        if df.empty:
            logger.warning(f"DataFrame vacío para {filepath}")
            return []
        
        # AÑADIR: Verificar columnas mínimas requeridas
        required_cols = ['time', 'deviceInfo.deviceName']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Columnas faltantes en {filepath}: {missing_cols}")
            return []
        
        
        # Filtrar filas con datos esenciales
        essential_cols = ['object.co2', 'object.temperature', 'object.humidity']
        df = df.dropna(subset=essential_cols, how='all')
        
        # Limpiar y transformar
        records = []
        for _, row in df.iterrows():
            record = {
                'sensor_type': 'aire',
                'device_name': str(row.get('deviceInfo.deviceName', '')),
                'timestamp': pd.to_datetime(row.get('time')).isoformat() if pd.notna(row.get('time')) else None,
                'latitude': self._extract_coordinate(row.get('deviceInfo.tags.Location'), 0),
                'longitude': self._extract_coordinate(row.get('deviceInfo.tags.Location'), 1),
                'co2': self._validate_co2(row.get('object.co2')),
                'temperature': self._validate_temperature(row.get('object.temperature')),
                'humidity': self._validate_humidity(row.get('object.humidity')),
                'pressure': self._validate_pressure(row.get('object.pressure')),
                'battery': self._validate_battery(row.get('object.battery')),
                'data_quality': 'good'  # Base, se ajustará en consumer
            }
            records.append(record)
        
        logger.info(f"Procesados {len(records)} registros de aire")
        return records
    
    def _process_sound_data(self, filepath):
        """Procesar datos de sonido"""
        df = pd.read_csv(filepath, low_memory=False)
        
        # Filtrar solo mensajes de datos (fPort = 85)
        df = df[df['fPort'] == 85]
        
        records = []
        for _, row in df.iterrows():
            record = {
                'sensor_type': 'sonido',
                'device_name': str(row.get('deviceInfo.deviceName', '')),
                'timestamp': pd.to_datetime(row.get('time')).isoformat() if pd.notna(row.get('time')) else None,
                'latitude': self._extract_coordinate(row.get('deviceInfo.tags.Location'), 0),
                'longitude': self._extract_coordinate(row.get('deviceInfo.tags.Location'), 1),
                'laeq': self._validate_laeq(row.get('object.LAeq')),
                'lai': row.get('object.LAI'),
                'laimax': row.get('object.LAImax'),
                'status': str(row.get('object.status', '')),
                'battery': self._validate_battery(row.get('object.battery')),
                'data_quality': 'good'
            }
            records.append(record)
        
        logger.info(f"Procesados {len(records)} registros de sonido")
        return records
    
    def _process_water_data(self, filepath):
        """Procesar datos de agua"""
        df = pd.read_csv(filepath, low_memory=False)
        
        records = []
        for _, row in df.iterrows():
            # Calcular nivel de agua desde distancia
            distance = row.get('object.distance')
            water_level = None
            if pd.notna(distance):
                water_level = max(0, min(100, 100 - (float(distance) / 100 * 100)))
            
            record = {
                'sensor_type': 'agua',
                'device_name': str(row.get('deviceInfo.deviceName', '')),
                'timestamp': pd.to_datetime(row.get('time')).isoformat() if pd.notna(row.get('time')) else None,
                'latitude': self._extract_coordinate(row.get('deviceInfo.tags.Location'), 0),
                'longitude': self._extract_coordinate(row.get('deviceInfo.tags.Location'), 1),
                'water_level': self._validate_water_level(water_level),
                'distance': distance,
                'status': str(row.get('object.status', '')),
                'code': str(row.get('code', '')),
                'battery': self._validate_battery(row.get('object.battery')),
                'data_quality': 'good'
            }
            records.append(record)
        
        logger.info(f"Procesados {len(records)} registros de agua")
        return records
    
    def get_batch(self, sensor_type, batch_size=50):
        """Obtener un lote de datos para enviar"""
        all_data = self.load_and_process_data(sensor_type)
        
        if not all_data:
            return []
        
        # Tomar un lote aleatorio para simular streaming
        if len(all_data) > batch_size:
            import random
            batch = random.sample(all_data, batch_size)
        else:
            batch = all_data
        
        return batch
    
    # Métodos de validación
    def _validate_co2(self, value):
        if pd.isna(value) or value < 300 or value > 5000:
            return None
        return float(value)
    
    def _validate_temperature(self, value):
        if pd.isna(value) or value < -10 or value > 50:
            return None
        return float(value)
    
    def _validate_humidity(self, value):
        if pd.isna(value) or value < 0 or value > 100:
            return None
        return float(value)
    
    def _validate_pressure(self, value):
        if pd.isna(value) or value < 500 or value > 1100:
            return None
        return float(value)
    
    def _validate_battery(self, value):
        if pd.isna(value) or value < 0 or value > 100:
            return None
        return float(value)
    
    def _validate_laeq(self, value):
        if pd.isna(value) or value < 30 or value > 120:
            return None
        return float(value)
    
    def _validate_water_level(self, value):
        if pd.isna(value) or value < 0 or value > 100:
            return None
        return float(value)
    
    def _extract_coordinate(self, location_str, index):
        """Extraer coordenadas del string de ubicación"""
        try:
            if pd.isna(location_str):
                return None
            cleaned = str(location_str).strip('" ')
            parts = cleaned.split(',')
            return float(parts[index].strip())
        except:
            return None