# consumer/etl_processor.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLProcessor:
    """Procesador ETL para limpieza y transformación de datos IoT"""
    
    @staticmethod
    def process_air_data(data):
        """Procesar datos de aire con validaciones y transformaciones"""
        processed = data.copy()
        
        # 1. Validaciones básicas
        processed = ETLProcessor._validate_air_data(processed)
        
        # 2. Calcular punto de rocío si hay temperatura y humedad
        if processed.get('temperature') and processed.get('humidity'):
            processed['dew_point'] = ETLProcessor._calculate_dew_point(
                processed['temperature'], 
                processed['humidity']
            )
        
        # 3. Categorizar calidad del aire
        if processed.get('co2'):
            processed['air_quality_category'] = ETLProcessor._categorize_air_quality(processed['co2'])
        
        # 4. Categorizar temperatura
        if processed.get('temperature'):
            processed['temperature_category'] = ETLProcessor._categorize_temperature(processed['temperature'])
        
        # 5. Calcular calidad de datos
        processed['data_quality'] = ETLProcessor._calculate_air_data_quality(processed)
        
        return processed
    
    @staticmethod
    def process_sound_data(data):
        """Procesar datos de sonido"""
        processed = data.copy()
        
        # 1. Validar LAeq
        if processed.get('laeq'):
            if processed['laeq'] < 30 or processed['laeq'] > 120:
                logger.warning(f"LAeq fuera de rango: {processed['laeq']}")
                processed['laeq'] = None
        
        # 2. Imputar LAeq si está vacío
        if processed.get('laeq') is None and processed.get('lai'):
            processed['laeq'] = processed['lai']
        
        # 3. Categorizar ruido
        if processed.get('laeq'):
            processed['noise_category'] = ETLProcessor._categorize_noise(processed['laeq'])
        
        # 4. Calcular variabilidad
        if processed.get('laimax') and processed.get('laeq'):
            processed['noise_variation'] = processed['laimax'] - processed['laeq']
        
        return processed
    
    @staticmethod
    def process_water_data(data):
        """Procesar datos de agua"""
        processed = data.copy()
        
        # 1. Calcular nivel de agua si no existe pero hay distancia
        if processed.get('water_level') is None and processed.get('distance'):
            processed['water_level'] = ETLProcessor._distance_to_percentage(processed['distance'])
        
        # 2. Validar nivel de agua
        if processed.get('water_level'):
            if processed['water_level'] < 0 or processed['water_level'] > 100:
                logger.warning(f"Nivel de agua fuera de rango: {processed['water_level']}")
                processed['water_level'] = None
        
        # 3. Interpretar código de estado
        if processed.get('code'):
            code_str = str(processed['code']).lower()
            if 'lleno' in code_str:
                processed['estimated_level'] = 90
            elif 'medio' in code_str:
                processed['estimated_level'] = 50
            elif 'bajo' in code_str:
                processed['estimated_level'] = 20
        
        # 4. Categorizar estado del tanque
        if processed.get('water_level'):
            processed['tank_status'] = ETLProcessor._categorize_tank_status(processed['water_level'])
        
        return processed
    
    @staticmethod
    def add_time_features(timestamp):
        """Agregar características temporales para análisis"""
        if not timestamp:
            return {}
        
        try:
            if isinstance(timestamp, str):
                dt = pd.to_datetime(timestamp)
            else:
                dt = timestamp
            
            # Características cíclicas para modelos
            hour_rad = 2 * np.pi * dt.hour / 24
            
            return {
                'hour': dt.hour,
                'day_of_week': dt.dayofweek,
                'month': dt.month,
                'is_weekend': 1 if dt.dayofweek >= 5 else 0,
                'is_night': 1 if 22 <= dt.hour or dt.hour < 6 else 0,
                'is_rush_hour': 1 if (7 <= dt.hour <= 9) or (17 <= dt.hour <= 19) else 0,
                'hour_sin': np.sin(hour_rad),
                'hour_cos': np.cos(hour_rad),
            }
        except Exception as e:
            logger.error(f"Error procesando timestamp: {e}")
            return {}
    
    # Métodos auxiliares privados
    @staticmethod
    def _validate_air_data(data):
        """Validar datos de aire"""
        validated = data.copy()
        
        # CO2
        if validated.get('co2'):
            if validated['co2'] < 300 or validated['co2'] > 5000:
                validated['co2'] = None
        
        # Temperatura
        if validated.get('temperature'):
            if validated['temperature'] < -10 or validated['temperature'] > 50:
                validated['temperature'] = None
        
        # Humedad
        if validated.get('humidity'):
            if validated['humidity'] < 0 or validated['humidity'] > 100:
                validated['humidity'] = None
        
        # Presión
        if validated.get('pressure'):
            if validated['pressure'] < 500 or validated['pressure'] > 1100:
                validated['pressure'] = None
        
        return validated
    
    @staticmethod
    def _calculate_dew_point(temperature, humidity):
        """Calcular punto de rocío usando fórmula de Magnus"""
        try:
            # AÑADIR validación para evitar log(0)
            if humidity <= 0 or temperature is None or humidity is None:
                return None
            a = 17.27
            b = 237.7
            alpha = ((a * temperature) / (b + temperature)) + np.log(humidity/100.0)
            dew_point = (b * alpha) / (a - alpha)
            return round(dew_point, 2)
        except:
            return None
    
    @staticmethod
    def _categorize_air_quality(co2):
        """Categorizar calidad del aire basado en CO2"""
        if co2 < 450:
            return "Excelente"
        elif co2 < 600:
            return "Buena"
        elif co2 < 1000:
            return "Moderada"
        elif co2 < 2000:
            return "Pobre"
        else:
            return "Peligrosa"
    
    @staticmethod
    def _categorize_temperature(temp):
        """Categorizar temperatura"""
        if temp < 15:
            return "Frío"
        elif temp < 22:
            return "Fresco"
        elif temp < 26:
            return "Confortable"
        elif temp < 30:
            return "Cálido"
        else:
            return "Caluroso"
    
    @staticmethod
    def _categorize_noise(laeq):
        """Categorizar nivel de ruido"""
        if laeq < 50:
            return "Silencioso"
        elif laeq < 65:
            return "Moderado"
        elif laeq < 75:
            return "Ruidoso"
        elif laeq < 85:
            return "Muy ruidoso"
        else:
            return "Peligroso"
    
    @staticmethod
    def _categorize_tank_status(water_level):
        """Categorizar estado del tanque"""
        if water_level < 20:
            return "Crítico"
        elif water_level < 40:
            return "Bajo"
        elif water_level < 60:
            return "Medio"
        elif water_level < 80:
            return "Alto"
        else:
            return "Lleno"
    
    @staticmethod
    def _distance_to_percentage(distance):
        """Convertir distancia a porcentaje"""
        try:
            distance = float(distance)
            # Suponiendo que 0 cm = 100% y 100 cm = 0%
            percentage = max(0, min(100, 100 - (distance / 100 * 100)))
            return round(percentage, 2)
        except:
            return None
    
    @staticmethod
    def _calculate_air_data_quality(data):
        """Calcular score de calidad de datos de aire"""
        score = 0
        total_possible = 0
        
        # CO2 (20 puntos)
        if data.get('co2') is not None:
            score += 20
        total_possible += 20
        
        # Temperatura (20 puntos)
        if data.get('temperature') is not None:
            score += 20
        total_possible += 20
        
        # Humedad (20 puntos)
        if data.get('humidity') is not None:
            score += 20
        total_possible += 20
        
        # Presión (10 puntos - opcional)
        if data.get('pressure') is not None:
            score += 10
        total_possible += 10
        
        # Batería (10 puntos)
        if data.get('battery') is not None:
            score += 10
        total_possible += 10
        
        # Ubicación (20 puntos)
        if data.get('latitude') is not None and data.get('longitude') is not None:
            score += 20
        total_possible += 20
        
        # Calcular porcentaje
        if total_possible > 0:
            quality_percentage = (score / total_possible) * 100
            
            if quality_percentage >= 90:
                return "excelente"
            elif quality_percentage >= 70:
                return "buena"
            elif quality_percentage >= 50:
                return "moderada"
            else:
                return "pobre"
        
        return "desconocida"