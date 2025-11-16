import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self):
        self.stats = {
            'total_processed': 0,
            'valid_records': 0,
            'empty_records': 0,
            'invalid_records': 0,
            'rejection_reasons': {}
        }
    
    def is_completely_empty(self, record: Dict[str, Any]) -> bool:
        """Verifica si un registro está completamente vacío (solo comas)"""
        if not record:
            return True
            
        # Contar valores no vacíos
        non_empty_count = sum(
            1 for value in record.values() 
            if value is not None and value != '' and not pd.isna(value)
        )
        
        return non_empty_count == 0
    
    def validate_sensor_record(self, record: Dict[str, Any], sensor_type: str) -> Tuple[bool, str]:
        """Valida un registro de sensor de manera exhaustiva"""
        self.stats['total_processed'] += 1
        
        # 1. Verificar si está completamente vacío
        if self.is_completely_empty(record):
            self.stats['empty_records'] += 1
            self._log_rejection("REGISTRO_COMPLETAMENTE_VACIO", record)
            return False, "Registro completamente vacío"
        
        # 2. Validar campos requeridos básicos
        required_fields = ['_id', 'time']
        missing_required = [field for field in required_fields if not record.get(field)]
        if missing_required:
            self.stats['invalid_records'] += 1
            reason = f"Campos requeridos faltantes: {missing_required}"
            self._log_rejection("CAMPOS_REQUERIDOS_FALTANTES", record)
            return False, reason
        
        # 3. Validar que tenga datos de medición específicos del sensor
        measurement_fields = self._get_measurement_fields(sensor_type)
        has_measurement = any(
            record.get(field) is not None and 
            record.get(field) != '' and 
            not pd.isna(record.get(field))
            for field in measurement_fields
        )
        
        if not has_measurement:
            self.stats['invalid_records'] += 1
            reason = f"Sin datos de medición para {sensor_type}"
            self._log_rejection("SIN_DATOS_MEDICION", record)
            return False, reason
        
        # 4. Validación específica por tipo de sensor
        if not self._validate_sensor_specific(record, sensor_type):
            self.stats['invalid_records'] += 1
            reason = f"Validación específica fallida para {sensor_type}"
            self._log_rejection("VALIDACION_ESPECIFICA_FALLIDA", record)
            return False, reason
        
        self.stats['valid_records'] += 1
        return True, "Válido"
    
    def _get_measurement_fields(self, sensor_type: str) -> List[str]:
        """Obtiene campos de medición por tipo de sensor"""
        fields = {
            'air_quality': [
                'object.co2', 'object.temperature', 'object.humidity', 'object.pressure',
                'object.co2_status', 'object.temperature_status', 'object.humidity_status'
            ],
            'sound': [
                'object.LAeq', 'object.LAI', 'object.LAImax', 'object.status'
            ],
            'water': [
                'object.distance', 'object.position', 'object.status', 'batteryLevel'
            ]
        }
        return fields.get(sensor_type, [])
    
    def _validate_sensor_specific(self, record: Dict[str, Any], sensor_type: str) -> bool:
        """Validaciones específicas por tipo de sensor"""
        try:
            if sensor_type == 'air_quality':
                # Validar que al menos un campo numérico tenga valor razonable
                numeric_fields = ['object.co2', 'object.temperature', 'object.humidity', 'object.pressure']
                has_valid_numeric = any(
                    self._is_reasonable_value(record.get(field), field) 
                    for field in numeric_fields
                )
                return has_valid_numeric
            
            elif sensor_type == 'sound':
                # Validar niveles de sonido
                laeq = record.get('object.LAeq')
                if laeq is not None:
                    return self._is_reasonable_value(laeq, 'sound_level')
                return True
                
            elif sensor_type == 'water':
                # Validar distancia o posición
                distance = record.get('object.distance')
                position = record.get('object.position')
                return (distance is not None and self._is_reasonable_value(distance, 'distance')) or position is not None
            
            return True
            
        except Exception as e:
            logger.warning(f"Error en validación específica: {e}")
            return True  # No bloquear por errores de validación específica
    
    def _is_reasonable_value(self, value, field_type: str) -> bool:
        """Verifica si un valor está en un rango razonable"""
        if value is None or value == '':
            return False
        
        try:
            num_value = float(value)
            
            ranges = {
                'object.co2': (300, 5000),        # CO2 en ppm
                'object.temperature': (-50, 60),  # Temperatura en °C
                'object.humidity': (0, 100),      # Humedad en %
                'object.pressure': (500, 1100),   # Presión en hPa
                'sound_level': (30, 120),         # Nivel sonoro en dB
                'distance': (0, 100)              # Distancia en metros
            }
            
            min_val, max_val = ranges.get(field_type, (float('-inf'), float('inf')))
            return min_val <= num_value <= max_val
            
        except (ValueError, TypeError):
            return False
    
    def _log_rejection(self, reason: str, record: Dict[str, Any]):
        """Registra razones de rechazo"""
        self.stats['rejection_reasons'][reason] = self.stats['rejection_reasons'].get(reason, 0) + 1
        
        # Log detallado solo para los primeros rechazos de cada tipo
        if self.stats['rejection_reasons'][reason] <= 3:
            record_id = record.get('_id', 'Unknown')
            logger.warning(f"❌ Registro rechazado - Razón: {reason}, ID: {record_id}")
    
    def get_validation_report(self) -> Dict[str, Any]:
        """Genera reporte completo de validación"""
        total = self.stats['total_processed']
        return {
            'total_processed': total,
            'valid_records': self.stats['valid_records'],
            'empty_records': self.stats['empty_records'],
            'invalid_records': self.stats['invalid_records'],
            'validity_rate': self.stats['valid_records'] / total if total > 0 else 0,
            'empty_rate': self.stats['empty_records'] / total if total > 0 else 0,
            'rejection_reasons': self.stats['rejection_reasons']
        }