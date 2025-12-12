import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import joblib
import os
# Agregar en las importaciones:
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score

import traceback

from sklearn.metrics import (
    mean_absolute_error, mean_squared_error, r2_score,
    explained_variance_score, mean_absolute_percentage_error,
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report
)
import matplotlib.pyplot as plt
import seaborn as sns

# XGBoost para mejor performance
XGBOOST_AVAILABLE = False

from app.redis_client import RedisClient
from app.config import MLConfig


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MLPredictor:
    """Sistema de predicción para sensores IoT"""
    
    def __init__(self, redis_client: RedisClient, config: MLConfig):
        self.redis = redis_client
        self.config = config
        self.models = {}
        self.scalers = {}
        self.feature_importances = {}
        
        # Crear directorio de modelos
        os.makedirs(config.models_path, exist_ok=True)
        if not self.redis.client:
            logger.warning("⚠️ Redis no está conectado. El MLPredictor funcionará en modo limitado.")
        else:
            logger.info(f"✅ MLPredictor inicializado para modelos en: {config.models_path}")
    
    # Reemplazar la función actual por esta versión completa
    def _extract_data_for_sensor(self, sensor_type: str) -> pd.DataFrame:
        """Extraer y limpiar datos para un tipo de sensor"""
        logger.info(f"Extrayendo datos para sensor: {sensor_type}")
        
        # Obtener datos de Redis
        raw_data = self.redis.get_all_sensor_data(sensor_type, limit_per_device=200)
        
        if not raw_data:
            logger.warning(f"No hay datos para {sensor_type}")
            return pd.DataFrame()
        
        # Convertir a DataFrame
        records = []
        for item in raw_data:
            try:
                # Asegurar que tenemos el campo 'data'
                if 'data' not in item:
                    # Si no hay 'data', usar todo el item como datos
                    data = {k: v for k, v in item.items() 
                        if k not in ['timestamp', 'device_name']}
                    timestamp = item.get('timestamp')
                    device_name = item.get('device_name')
                else:
                    data = item.get('data', {})
                    if isinstance(data, str):
                        try:
                            data = json.loads(data)
                        except:
                            data = {}
                    timestamp = item.get('timestamp')
                    device_name = item.get('device_name')
                
                # Crear registro
                record = {
                    'timestamp': timestamp,
                    'device_name': device_name,
                }
                
                # Agregar campos según tipo de sensor
                if sensor_type == 'aire':
                    fields = ['co2', 'temperature', 'humidity', 'pressure', 'battery']
                elif sensor_type == 'sonido':
                    fields = ['laeq', 'lai', 'laimax', 'battery']
                elif sensor_type == 'agua':
                    fields = ['water_level', 'distance', 'battery']
                else:
                    fields = []
                
                # Agregar campos existentes
                for field in fields:
                    if field in data:
                        try:
                            # Convertir a float si es posible
                            value = data[field]
                            if isinstance(value, str):
                                # Limpiar strings
                                value = value.replace(',', '.')
                            record[field] = float(value) if value not in [None, ''] else None
                        except:
                            record[field] = None
                    else:
                        record[field] = None
                
                records.append(record)
                
            except Exception as e:
                logger.warning(f"Error procesando registro: {e}")
                continue
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        
        # Convertir timestamp a datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        # Eliminar filas sin timestamp
        df = df.dropna(subset=['timestamp'])
        
        # Ordenar por timestamp
        df = df.sort_values('timestamp')
        
        logger.info(f"Datos extraídos: {len(df)} registros para {sensor_type}")
        return df
    
    def _clean_air_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpiar datos de aire"""
        logger.info(f"Limpiando datos de aire. Registros iniciales: {len(df)}")



        # Convertir columnas numéricas
        numeric_cols = ['co2', 'temperature', 'humidity', 'pressure', 'battery']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                logger.info(f"Columna {col}: {df[col].notna().sum()} valores no nulos")
        
        # Si no hay datos de CO2, no podemos entrenar
        if 'co2' not in df.columns or df['co2'].isna().all():
            logger.error("No hay datos de CO2 para entrenar")
            return pd.DataFrame()
        
        # Eliminar filas sin CO2
        df = df.dropna(subset=['co2'])
        
        # Eliminar outliers extremos (basado en lógica de dominio)
        if 'co2' in df.columns:
            q1 = df['co2'].quantile(0.01)
            q3 = df['co2'].quantile(0.99)
            iqr = q3 - q1
            df = df[(df['co2'] >= q1 - 1.5*iqr) & (df['co2'] <= q3 + 1.5*iqr)]

        # Rellenar otros campos faltantes con valores razonables
        if 'temperature' in df.columns:
            df['temperature'] = df['temperature'].fillna(22.0)  # Temperatura ambiente promedio
        if 'humidity' in df.columns:
            df['humidity'] = df['humidity'].fillna(50.0)  # Humedad promedio
        
        logger.info(f"Datos limpios: {len(df)} registros")
        
        # Eliminar filas con valores críticos nulos
        # df = df.dropna(subset=['co2'], how='any')
        
        return df
    
    def _clean_sound_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpiar datos de sonido"""
        logger.info("Limpiando datos de sonido...")
        
        numeric_cols = ['laeq', 'lai', 'laimax', 'battery']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Eliminar outliers
        if 'laeq' in df.columns:
            q1 = df['laeq'].quantile(0.01)
            q3 = df['laeq'].quantile(0.99)
            iqr = q3 - q1
            df = df[(df['laeq'] >= q1 - 1.5*iqr) & (df['laeq'] <= q3 + 1.5*iqr)]
        
        df = df.dropna(subset=['laeq'], how='any')
        return df
    
    def _clean_water_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpiar datos de agua"""
        logger.info("Limpiando datos de agua...")
        
        numeric_cols = ['water_level', 'distance', 'battery']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Eliminar outliers
        if 'water_level' in df.columns:
            q1 = df['water_level'].quantile(0.01)
            q3 = df['water_level'].quantile(0.99)
            iqr = q3 - q1
            df = df[(df['water_level'] >= q1 - 1.5*iqr) & (df['water_level'] <= q3 + 1.5*iqr)]
        
        df = df.dropna(subset=['water_level'], how='any')
        return df
    
    def prepare_training_data(self, sensor_type: str) -> Tuple[pd.DataFrame, str]:
        """Preparar datos para entrenamiento"""
        # Obtener datos brutos
        df = self._extract_data_for_sensor(sensor_type)
        
        if df.empty:
            raise ValueError(f"No hay datos para {sensor_type}")
        
        # Limpiar datos
        df = self._clean_sensor_data(df, sensor_type)
        
        if len(df) < self.config.min_samples:
            raise ValueError(f"Datos insuficientes para {sensor_type}: {len(df)} registros (mínimo {self.config.min_samples})")
        
        # Definir columna objetivo
        if sensor_type == 'aire':
            target_col = 'co2'
        elif sensor_type == 'sonido':
            target_col = 'laeq'
        elif sensor_type == 'agua':
            target_col = 'water_level'
        else:
            raise ValueError(f"Tipo de sensor no válido: {sensor_type}")
        
        # Verificar que existe la columna objetivo
        if target_col not in df.columns:
            raise ValueError(f"No se encontró la columna objetivo {target_col} para {sensor_type}")
        
        # Crear features temporales si hay timestamp
        if 'timestamp' in df.columns:
            df['hour'] = df['timestamp'].dt.hour
            df['day_of_week'] = df['timestamp'].dt.dayofweek
            df['day_of_month'] = df['timestamp'].dt.day
            df['month'] = df['timestamp'].dt.month
            
            # Features cíclicas
            df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
            df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
            df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
            df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
            
            # Lag features (valores anteriores)
            if len(df) > 3:
                df[f'{target_col}_lag1'] = df[target_col].shift(1)
                df[f'{target_col}_lag2'] = df[target_col].shift(2)
                df[f'{target_col}_lag3'] = df[target_col].shift(3)
                
                # Rolling statistics
                df[f'{target_col}_rolling_mean_3'] = df[target_col].rolling(window=3, min_periods=1).mean()
                df[f'{target_col}_rolling_std_3'] = df[target_col].rolling(window=3, min_periods=1).std()
        
        # Eliminar filas con NaN
        df = df.dropna()
        
        logger.info(f"Datos preparados: {len(df)} registros, objetivo: {target_col}")
        return df, target_col
    
    def _calculate_comprehensive_metrics(self, y_true, y_pred, y_true_binary=None, y_pred_binary=None):
        """Calcular métricas completas para evaluación del modelo"""
        metrics = {}
        
        # Métricas de regresión
        metrics['r2'] = r2_score(y_true, y_pred)
        metrics['mae'] = mean_absolute_error(y_true, y_pred)
        metrics['mse'] = mean_squared_error(y_true, y_pred)
        metrics['rmse'] = np.sqrt(mean_squared_error(y_true, y_pred))
        metrics['mape'] = mean_absolute_percentage_error(y_true, y_pred)
        metrics['explained_variance'] = explained_variance_score(y_true, y_pred)
        
        # Métricas adicionales útiles
        metrics['max_error'] = np.max(np.abs(y_true - y_pred))
        metrics['mean_error'] = np.mean(y_true - y_pred)
        metrics['std_error'] = np.std(y_true - y_pred)
        
        # Si tenemos valores binarios para clasificación
        if y_true_binary is not None and y_pred_binary is not None:
            metrics['accuracy'] = accuracy_score(y_true_binary, y_pred_binary)
            metrics['precision'] = precision_score(y_true_binary, y_pred_binary, average='weighted', zero_division=0)
            metrics['recall'] = recall_score(y_true_binary, y_pred_binary, average='weighted', zero_division=0)
            metrics['f1'] = f1_score(y_true_binary, y_pred_binary, average='weighted', zero_division=0)
            
            # Matriz de confusión
            cm = confusion_matrix(y_true_binary, y_pred_binary)
            metrics['confusion_matrix'] = cm.tolist()
            
            # Reporte de clasificación
            report = classification_report(y_true_binary, y_pred_binary, output_dict=True)
            metrics['classification_report'] = report
            
            # Métricas específicas para problemas binarios
            if len(np.unique(y_true_binary)) == 2:
                tn, fp, fn, tp = cm.ravel()
                metrics['specificity'] = tn / (tn + fp) if (tn + fp) > 0 else 0
                metrics['false_positive_rate'] = fp / (fp + tn) if (fp + tn) > 0 else 0
                metrics['false_negative_rate'] = fn / (fn + tp) if (fn + tp) > 0 else 0
        
        return metrics
    
    def train_model(self, sensor_type: str) -> Dict:
        """Entrenar modelo con métricas completas"""
        logger.info(f"🚀 Entrenando modelo para {sensor_type}")
        logger.info(f"Configuración ML: debug={self.config.debug}, min_samples={self.config.min_samples}")
        
        try:
            # Preparar datos (código existente)
            df, target_col = self.prepare_training_data(sensor_type)
            
            if len(df) < self.config.min_samples:
                return {
                    'success': False,
                    'sensor_type': sensor_type,
                    'error': f'Datos insuficientes: {len(df)} registros (mínimo {self.config.min_samples})'
                }
            
            # Crear variable binaria para clasificación (umbral automático)
            threshold = df[target_col].median()  # Umbral por mediana
            df[f'{target_col}_binary'] = (df[target_col] > threshold).astype(int)
            
            # Features (código existente con mejoras)
            feature_cols = [
                'hour', 'day_of_week', 'day_of_month', 'month',
                'hour_sin', 'hour_cos', 'day_sin', 'day_cos',
                f'{target_col}_lag1', f'{target_col}_lag2', f'{target_col}_lag3',
                f'{target_col}_rolling_mean_3', f'{target_col}_rolling_std_3'
            ]
            
            # Filtrar columnas existentes y sin NaN
            feature_cols = [col for col in feature_cols if col in df.columns]
            df_features = df[feature_cols].fillna(method='ffill').fillna(method='bfill')
            
            X = df_features.values
            y_reg = df[target_col].values
            y_clf = df[f'{target_col}_binary'].values
            
            # Split train/test
            X_train, X_test, y_train_reg, y_test_reg, y_train_clf, y_test_clf = train_test_split(
                X, y_reg, y_clf, test_size=0.2, random_state=42
            )
            
            # Escalar
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            # Modelo de regresión
            reg_model = RandomForestRegressor(
                n_estimators=50,  # Reducido para velocidad
                max_depth=10,
                random_state=42,
                n_jobs=-1,
                min_samples_split=10
            )
            reg_model.fit(X_train_scaled, y_train_reg)
            
            # Modelo de clasificación
            clf_model = RandomForestClassifier(
                n_estimators=50,
                max_depth=10,
                random_state=42,
                n_jobs=-1,
                min_samples_split=10
            )
            clf_model.fit(X_train_scaled, y_train_clf)
            
            # Predicciones
            y_pred_reg = reg_model.predict(X_test_scaled)
            y_pred_clf = clf_model.predict(X_test_scaled)
            
            # Calcular TODAS las métricas
            metrics = self._calculate_comprehensive_metrics(
                y_test_reg, y_pred_reg, y_test_clf, y_pred_clf
            )
            
            # Feature importances
            feature_importances = dict(zip(feature_cols, reg_model.feature_importances_))
            
            # Guardar modelos
            model_path = os.path.join(self.config.models_path, f'{sensor_type}_model.joblib')
            scaler_path = os.path.join(self.config.models_path, f'{sensor_type}_scaler.joblib')
            clf_path = os.path.join(self.config.models_path, f'{sensor_type}_clf_model.joblib')
            
            joblib.dump(reg_model, model_path)
            joblib.dump(scaler, scaler_path)
            joblib.dump(clf_model, clf_path)
            
            # Calcular percentiles para interpretación
            percentiles = {
                'p10': np.percentile(y_test_reg, 10),
                'p25': np.percentile(y_test_reg, 25),
                'p50': np.percentile(y_test_reg, 50),
                'p75': np.percentile(y_test_reg, 75),
                'p90': np.percentile(y_test_reg, 90)
            }
            
            return {
                'success': True,
                'sensor_type': sensor_type,
                'metrics': metrics,
                'feature_importances': feature_importances,
                'samples': {
                    'total': len(df),
                    'train': len(X_train),
                    'test': len(X_test)
                },
                'percentiles': percentiles,
                'threshold_used': float(threshold),
                'model_info': {
                    'regression': 'RandomForestRegressor',
                    'classification': 'RandomForestClassifier',
                    'features_used': feature_cols
                },
                'files': {
                    'regression_model': model_path,
                    'scaler': scaler_path,
                    'classification_model': clf_path
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error entrenando modelo: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                'success': False,
                'sensor_type': sensor_type,
                'error': str(e),
                'traceback': traceback.format_exc() if self.config.debug else None
            }
    
    def predict(self, sensor_type: str, days: int = None) -> Dict:
        """Predecir valores futuros"""
        if days is None:
            days = self.config.prediction_days
        
        logger.info(f"🔮 Prediciendo {days} días para {sensor_type}")
        
        try:
            # Cargar modelo si no está en memoria
            if sensor_type not in self.models:
                model_path = os.path.join(self.config.models_path, f'{sensor_type}_model.joblib')
                scaler_path = os.path.join(self.config.models_path, f'{sensor_type}_scaler.joblib')
                
                if not os.path.exists(model_path):
                    return {
                        'success': False,
                        'error': f'Modelo para {sensor_type} no encontrado. Entrena primero.'
                    }
                
                self.models[sensor_type] = joblib.load(model_path)
                self.scalers[sensor_type] = joblib.load(scaler_path)
            
            # Obtener datos históricos recientes
            df, target_col = self.prepare_training_data(sensor_type)
            
            if df.empty:
                return {'success': False, 'error': 'No hay datos para predecir'}
            
            # Preparar features para predicción
            last_record = df.iloc[-1].copy()
            last_date = df['timestamp'].max()
            
            predictions = []
            
            for i in range(1, days + 1):
                future_date = last_date + timedelta(days=i)
                
                # Actualizar features temporales
                last_record['hour'] = future_date.hour
                last_record['day_of_week'] = future_date.dayofweek
                last_record['day_of_month'] = future_date.day
                last_record['month'] = future_date.month
                last_record['hour_sin'] = np.sin(2 * np.pi * future_date.hour / 24)
                last_record['hour_cos'] = np.cos(2 * np.pi * future_date.hour / 24)
                last_record['day_sin'] = np.sin(2 * np.pi * future_date.dayofweek / 7)
                last_record['day_cos'] = np.cos(2 * np.pi * future_date.dayofweek / 7)
                
                # Preparar features
                feature_cols = [
                    'hour', 'day_of_week', 'day_of_month', 'month',
                    'hour_sin', 'hour_cos', 'day_sin', 'day_cos',
                    f'{target_col}_lag1', f'{target_col}_lag2', f'{target_col}_lag3',
                    f'{target_col}_rolling_mean_3', f'{target_col}_rolling_std_3'
                ]
                
                feature_cols = [col for col in feature_cols if col in last_record.index]
                X_pred = last_record[feature_cols].values.reshape(1, -1)
                
                # Escalar y predecir
                X_pred_scaled = self.scalers[sensor_type].transform(X_pred)
                prediction = self.models[sensor_type].predict(X_pred_scaled)[0]
                
                predictions.append({
                    'date': future_date.strftime('%Y-%m-%d'),
                    'predicted_value': float(prediction),
                    'day': i
                })
                
                # Actualizar lags para siguiente predicción
                last_record[f'{target_col}_lag3'] = last_record[f'{target_col}_lag2']
                last_record[f'{target_col}_lag2'] = last_record[f'{target_col}_lag1']
                last_record[f'{target_col}_lag1'] = prediction
            
            return {
                'success': True,
                'sensor_type': sensor_type,
                'last_measurement_date': last_date.strftime('%Y-%m-%d %H:%M:%S'),
                'prediction_days': days,
                'predictions': predictions,
                'summary': {
                    'avg': np.mean([p['predicted_value'] for p in predictions]),
                    'min': np.min([p['predicted_value'] for p in predictions]),
                    'max': np.max([p['predicted_value'] for p in predictions]),
                    'trend': 'increasing' if predictions[-1]['predicted_value'] > predictions[0]['predicted_value'] else 'decreasing'
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error en predicción: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_model_info(self, sensor_type: str) -> Dict:
        """Obtener información del modelo"""
        model_path = os.path.join(self.config.models_path, f'{sensor_type}_model.joblib')
        
        if os.path.exists(model_path):
            return {
                'exists': True,
                'sensor_type': sensor_type,
                'model_path': model_path,
                'last_modified': datetime.fromtimestamp(os.path.getmtime(model_path)).isoformat()
            }
        else:
            return {
                'exists': False,
                'sensor_type': sensor_type
            }
    
    def _clean_sensor_data(self, df: pd.DataFrame, sensor_type: str) -> pd.DataFrame:
        """Limpieza general de datos de sensores"""
        if df.empty:
            return df
        
        logger.info(f"Limpiando datos de {sensor_type}. Registros iniciales: {len(df)}")
        
        # Definir columnas por tipo de sensor
        if sensor_type == 'aire':
            numeric_cols = ['co2', 'temperature', 'humidity', 'pressure', 'battery']
            target_col = 'co2'
        elif sensor_type == 'sonido':
            numeric_cols = ['laeq', 'lai', 'laimax', 'battery']
            target_col = 'laeq'
        elif sensor_type == 'agua':
            numeric_cols = ['water_level', 'distance', 'battery']
            target_col = 'water_level'
        else:
            return df
        
        # Convertir columnas numéricas
        for col in numeric_cols:
            if col in df.columns:
                # Reemplazar comas por puntos y convertir
                df[col] = df[col].replace({',': '.'}, regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                logger.info(f"  {col}: {df[col].notna().sum()} valores válidos")
        
        # Eliminar filas sin el valor objetivo
        if target_col in df.columns:
            df = df.dropna(subset=[target_col])
            logger.info(f"  Después de eliminar NaN en {target_col}: {len(df)} registros")
        
        # Eliminar outliers usando percentiles robustos
        if target_col in df.columns and len(df) > 10:
            q1 = df[target_col].quantile(0.05)  # Percentil 5
            q3 = df[target_col].quantile(0.95)  # Percentil 95
            df = df[(df[target_col] >= q1) & (df[target_col] <= q3)]
            logger.info(f"  Después de eliminar outliers: {len(df)} registros")
        
        # Rellenar valores faltantes para otras columnas
        for col in numeric_cols:
            if col != target_col and col in df.columns:
                df[col] = df[col].fillna(df[col].median())
        
        logger.info(f"Datos limpios: {len(df)} registros")
        return df


    def get_model_performance_dashboard(self, sensor_type: str) -> Dict:
        """Obtener dashboard completo de performance del modelo"""
        try:
            model_path = os.path.join(self.config.models_path, f'{sensor_type}_model.joblib')
            
            if not os.path.exists(model_path):
                return {
                    'exists': False,
                    'sensor_type': sensor_type,
                    'message': 'Modelo no encontrado, debe entrenar primero'
                }
            
            # Obtener datos actuales
            df, target_col = self.prepare_training_data(sensor_type)
            
            if df.empty:
                return {'error': 'No hay datos para evaluar'}
            
            # Preparar features
            feature_cols = [col for col in [
                'hour', 'day_of_week', 'day_of_month', 'month',
                'hour_sin', 'hour_cos', 'day_sin', 'day_cos',
                f'{target_col}_lag1', f'{target_col}_lag2', f'{target_col}_lag3',
                f'{target_col}_rolling_mean_3', f'{target_col}_rolling_std_3'
            ] if col in df.columns]
            
            df_features = df[feature_cols].fillna(method='ffill').fillna(method='bfill')
            X = df_features.values
            y = df[target_col].values
            
            # Cargar modelo y scaler
            model = joblib.load(model_path)
            scaler = joblib.load(os.path.join(self.config.models_path, f'{sensor_type}_scaler.joblib'))
            
            X_scaled = scaler.transform(X)
            predictions = model.predict(X_scaled)
            
            # Calcular errores por percentil
            errors = predictions - y
            error_percentiles = {
                'abs_error_p10': np.percentile(np.abs(errors), 10),
                'abs_error_p50': np.percentile(np.abs(errors), 50),
                'abs_error_p90': np.percentile(np.abs(errors), 90),
                'max_abs_error': np.max(np.abs(errors))
            }
            
            # Distribución de errores
            error_distribution = {
                'within_5%': np.mean(np.abs(errors) < 0.05 * np.abs(y)) * 100,
                'within_10%': np.mean(np.abs(errors) < 0.10 * np.abs(y)) * 100,
                'within_20%': np.mean(np.abs(errors) < 0.20 * np.abs(y)) * 100
            }
            
            return {
                'exists': True,
                'sensor_type': sensor_type,
                'performance': {
                    'current_rmse': np.sqrt(np.mean(errors**2)),
                    'current_mae': np.mean(np.abs(errors)),
                    'current_r2': r2_score(y, predictions),
                    'error_distribution': error_distribution,
                    'error_percentiles': error_percentiles
                },
                'data_summary': {
                    'total_samples': len(y),
                    'date_range': {
                        'start': df['timestamp'].min().isoformat(),
                        'end': df['timestamp'].max().isoformat()
                    },
                    'value_range': {
                        'min': float(np.min(y)),
                        'max': float(np.max(y)),
                        'mean': float(np.mean(y)),
                        'std': float(np.std(y))
                    }
                },
                'predictions_summary': {
                    'predicted_min': float(np.min(predictions)),
                    'predicted_max': float(np.max(predictions)),
                    'predicted_mean': float(np.mean(predictions))
                }
            }
            
        except Exception as e:
            logger.error(f"Error en dashboard: {e}")
            return {'error': str(e)}
