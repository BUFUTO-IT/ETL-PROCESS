import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any
import logging
from schemas import AirQualityData, SoundData, WaterData
import os

logger = logging.getLogger(__name__)

class DataWarehouse:
    def __init__(self, output_dir: str = "data_warehouse"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Almacenamiento en memoria para simulación
        self.sensor_data = {
            'air_quality': [],
            'sound': [],
            'water': []
        }
        
        # Métricas de calidad
        self.quality_metrics = {
            'air_quality': {'total': 0, 'valid': 0, 'null_fields': {}},
            'sound': {'total': 0, 'valid': 0, 'null_fields': {}},
            'water': {'total': 0, 'valid': 0, 'null_fields': {}}
        }
    
    def add_sensor_data(self, sensor_type: str, data: Any):
        """Agrega datos de sensor al data warehouse"""
        try:
            self.sensor_data[sensor_type].append(data)
            self.quality_metrics[sensor_type]['total'] += 1
            
            # Analizar calidad de datos
            self._analyze_data_quality(sensor_type, data)
            
        except Exception as e:
            logger.error(f"❌ Error agregando datos de {sensor_type}: {e}")
    
    def _analyze_data_quality(self, sensor_type: str, data: Any):
        """Analiza la calidad de los datos ingresados"""
        if not data:
            return
            
        record_dict = vars(data) if hasattr(data, '__dict__') else data
        
        # Contar campos nulos/vacíos
        null_count = 0
        total_fields = 0
        
        for field, value in record_dict.items():
            total_fields += 1
            if value is None or value == '' or (isinstance(value, float) and np.isnan(value)):
                null_count += 1
                # Actualizar estadísticas por campo
                if field not in self.quality_metrics[sensor_type]['null_fields']:
                    self.quality_metrics[sensor_type]['null_fields'][field] = 0
                self.quality_metrics[sensor_type]['null_fields'][field] += 1
        
        # Considerar válido si tiene menos del 50% de campos nulos
        if null_count / total_fields < 0.5:
            self.quality_metrics[sensor_type]['valid'] += 1
    
    def generate_warehouse_report(self) -> Dict[str, Any]:
        """Genera reporte completo del data warehouse"""
        timestamp = datetime.now().isoformat()
        
        report = {
            'timestamp': timestamp,
            'summary': {},
            'sensor_reports': {},
            'data_quality': {},
            'recommendations': []
        }
        
        # Resumen por sensor
        for sensor_type in self.sensor_data:
            total_records = len(self.sensor_data[sensor_type])
            report['summary'][sensor_type] = {
                'total_records': total_records,
                'storage_size': f"{(total_records * 0.5):.2f} KB estimados"
            }
            
            # Reporte detallado por sensor
            if total_records > 0:
                report['sensor_reports'][sensor_type] = self._generate_sensor_report(sensor_type)
        
        # Calidad de datos
        report['data_quality'] = self._generate_quality_report()
        
        # Recomendaciones
        report['recommendations'] = self._generate_recommendations()
        
        return report
    
    def _generate_sensor_report(self, sensor_type: str) -> Dict[str, Any]:
        """Genera reporte específico para un tipo de sensor"""
        records = self.sensor_data[sensor_type]
        
        if not records:
            return {}
        
        # Convertir a DataFrame para análisis
        records_dict = [vars(record) if hasattr(record, '__dict__') else record for record in records]
        df = pd.DataFrame(records_dict)
        
        report = {
            'total_records': len(records),
            'time_range': {
                'start': df['time'].min() if 'time' in df.columns else 'N/A',
                'end': df['time'].max() if 'time' in df.columns else 'N/A'
            },
            'field_analysis': {},
            'data_issues': []
        }
        
        # Análisis por campo
        for column in df.columns:
            null_count = df[column].isna().sum()
            empty_count = (df[column].astype(str).str.strip() == '').sum()
            unique_count = df[column].nunique()
            
            report['field_analysis'][column] = {
                'null_count': int(null_count),
                'empty_count': int(empty_count),
                'unique_values': int(unique_count),
                'null_percentage': float((null_count / len(df)) * 100),
                'data_type': str(df[column].dtype)
            }
            
            # Detectar problemas
            if null_count / len(df) > 0.7:  # Más del 70% nulos
                report['data_issues'].append(f"Campo '{column}' tiene {null_count} valores nulos ({null_count/len(df)*100:.1f}%)")
        
        return report
    
    def _generate_quality_report(self) -> Dict[str, Any]:
        """Genera reporte de calidad de datos"""
        quality_report = {}
        
        for sensor_type, metrics in self.quality_metrics.items():
            total = metrics['total']
            valid = metrics['valid']
            
            quality_report[sensor_type] = {
                'total_records': total,
                'valid_records': valid,
                'validity_rate': valid / total if total > 0 else 0,
                'common_null_fields': dict(sorted(
                    metrics['null_fields'].items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )[:5])  # Top 5 campos más problemáticos
            }
        
        return quality_report
    
    def _generate_recommendations(self) -> List[str]:
        """Genera recomendaciones basadas en el análisis"""
        recommendations = []
        
        for sensor_type, metrics in self.quality_metrics.items():
            total = metrics['total']
            
            if total == 0:
                recommendations.append(f"⚠️ Sensor {sensor_type}: No se recibieron datos - verificar conectividad")
                continue
            
            validity_rate = metrics['valid'] / total
            
            if validity_rate < 0.8:
                recommendations.append(f"🔴 Sensor {sensor_type}: Baja tasa de validez ({validity_rate:.1%}) - revisar calidad de datos")
            elif validity_rate < 0.95:
                recommendations.append(f"🟡 Sensor {sensor_type}: Tasa de validez moderada ({validity_rate:.1%}) - mejorar calidad")
            else:
                recommendations.append(f"🟢 Sensor {sensor_type}: Excelente calidad de datos ({validity_rate:.1%})")
            
            # Recomendaciones específicas por campos nulos
            for field, null_count in metrics['null_fields'].items():
                null_percentage = (null_count / total) * 100
                if null_percentage > 50:
                    recommendations.append(f"📊 Sensor {sensor_type}: Campo '{field}' tiene {null_percentage:.1f}% valores nulos")
        
        if not recommendations:
            recommendations.append("✅ Todos los sensores están funcionando correctamente")
        
        return recommendations
    
    def export_to_json(self, filename: str = None):
        """Exporta todo el data warehouse a JSON"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.output_dir}/data_warehouse_{timestamp}.json"
        
        try:
            # Preparar datos para exportación
            export_data = {
                'metadata': {
                    'export_timestamp': datetime.now().isoformat(),
                    'total_records': sum(len(data) for data in self.sensor_data.values()),
                    'sensors_available': list(self.sensor_data.keys())
                },
                'warehouse_report': self.generate_warehouse_report(),
                'sensor_data_samples': {}
            }
            
            # Incluir muestras de datos (primeros 10 registros de cada sensor)
            for sensor_type, records in self.sensor_data.items():
                if records:
                    samples = [vars(record) if hasattr(record, '__dict__') else record 
                              for record in records[:10]]
                    export_data['sensor_data_samples'][sensor_type] = samples
            
            # Guardar archivo
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"💾 Data warehouse exportado a: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"❌ Error exportando data warehouse: {e}")
            return None
    
    def export_individual_sensor_json(self):
        """Exporta JSON individuales para cada sensor"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        exported_files = []
        
        for sensor_type, records in self.sensor_data.items():
            if records:
                filename = f"{self.output_dir}/{sensor_type}_data_{timestamp}.json"
                
                export_data = {
                    'sensor_type': sensor_type,
                    'export_timestamp': datetime.now().isoformat(),
                    'total_records': len(records),
                    'data_quality': self.quality_metrics[sensor_type],
                    'records': [vars(record) if hasattr(record, '__dict__') else record 
                               for record in records]
                }
                
                try:
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
                    
                    logger.info(f"💾 Datos de {sensor_type} exportados a: {filename}")
                    exported_files.append(filename)
                    
                except Exception as e:
                    logger.error(f"❌ Error exportando {sensor_type}: {e}")
        
        return exported_files