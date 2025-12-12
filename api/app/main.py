# main.py CORREGIDO
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import logging
from datetime import datetime

# NO importar RedisClient y MLPredictor aquí - eso causa el error
# En su lugar, importar solo las configuraciones
from app.config import RedisConfig, APIConfig, MLConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Crear aplicación FastAPI
app = FastAPI(
    title="IoT Monitoring API with Predictions",
    description="API para consultar datos de sensores IoT y hacer predicciones",
    version="2.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Variables globales - inicializar como None
redis_client = None
ml_predictor = None

@app.on_event("startup")
async def startup_event():
    """Inicializar servicios al iniciar"""
    global redis_client, ml_predictor
    
    from app.redis_client import RedisClient
    from app.ml_predictor import MLPredictor
    
    # Configurar Redis
    redis_config = RedisConfig()
    redis_client = RedisClient(redis_config)
    
    if not redis_client.connect():
        logger.error("❌ No se pudo conectar a Redis")
        logger.warning("Continuando sin Redis - algunas funciones estarán limitadas")
    else:
        logger.info("✅ Conectado a Redis exitosamente")
    
    # Configurar ML con debug mode
    ml_config = MLConfig()
    logger.info(f"ML Config: debug={ml_config.debug}, models_path={ml_config.models_path}")
    ml_predictor = MLPredictor(redis_client, ml_config)
    
    logger.info("🚀 API iniciada correctamente")

@app.on_event("shutdown")
async def shutdown_event():
    """Cerrar conexiones al apagar"""
    if redis_client:
        redis_client.close()
    logger.info("👋 API detenida")

# ==================== ENDPOINTS BÁSICOS ====================

@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "message": "IoT Monitoring API with Predictions",
        "version": "2.0.0",
        "endpoints": {
            "sensors": {
                "air": "/api/air",
                "sound": "/api/sound", 
                "water": "/api/water"
            },
            "history": "/api/{sensor_type}/history",
            "stats": "/api/stats/{sensor_type}",
            "ml": {
                "train": "/api/ml/train/{sensor_type}",
                "predict": "/api/ml/predict/{sensor_type}",
                "model_info": "/api/ml/info/{sensor_type}"
            }
        }
    }

@app.get("/health")
async def health_check():
    """Verificar salud de la API"""
    try:
        redis_status = "disconnected"
        if redis_client and redis_client.client:
            redis_client.client.ping()
            redis_status = "connected"
        
        return {
            "status": "healthy",
            "redis": redis_status,
            "ml_predictor": "initialized" if ml_predictor else "not_initialized",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "partial",
            "redis": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

# ==================== ENDPOINTS DE SENSORES ====================

@app.get("/api/air")
async def get_air_data(
    limit: int = Query(50, ge=1, le=500),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100)
):
    """Obtener datos de sensores de aire con paginación"""
    if not redis_client or not redis_client.client:
        raise HTTPException(status_code=503, detail="Redis no disponible")
    
    try:
        devices = redis_client.get_active_devices('aire') or []
        
        # Paginación
        total_devices = len(devices)
        total_pages = (total_devices + page_size - 1) // page_size if page_size > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_devices)
        
        paginated_devices = devices[start_idx:end_idx]
        
        result = []
        for device_name in paginated_devices:
            device_data = redis_client.get_device_data(device_name)
            history = redis_client.get_device_history('aire', device_name, limit=5)
            
            result.append({
                **device_data,
                "recent_measurements": history,
                "device_name": device_name
            })
        
        return {
            "sensor_type": "air",
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_devices": total_devices,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            },
            "data": result
        }
    except Exception as e:
        logger.error(f"Error en /api/air: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sound")
async def get_sound_data(limit: int = Query(50, ge=1, le=500)):
    """Obtener datos de sensores de sonido"""
    if not redis_client or not redis_client.client:
        raise HTTPException(status_code=503, detail="Redis no disponible")
    
    try:
        devices = redis_client.get_active_devices('sonido') or []
        
        result = []
        for device_name in devices[:limit]:
            device_data = redis_client.get_device_data(device_name)
            history = redis_client.get_device_history('sonido', device_name, limit=10)
            
            result.append({
                **device_data,
                "recent_measurements": history,
                "device_name": device_name
            })
        
        return {
            "sensor_type": "sound",
            "total_devices": len(devices),
            "data": result
        }
    except Exception as e:
        logger.error(f"Error en /api/sound: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/water")
async def get_water_data(limit: int = Query(50, ge=1, le=500)):
    """Obtener datos de sensores de agua"""
    if not redis_client or not redis_client.client:
        raise HTTPException(status_code=503, detail="Redis no disponible")
    
    try:
        devices = redis_client.get_active_devices('agua') or []
        
        result = []
        for device_name in devices[:limit]:
            device_data = redis_client.get_device_data(device_name)
            history = redis_client.get_device_history('agua', device_name, limit=10)
            
            result.append({
                **device_data,
                "recent_measurements": history,
                "device_name": device_name
            })
        
        return {
            "sensor_type": "water",
            "total_devices": len(devices),
            "data": result
        }
    except Exception as e:
        logger.error(f"Error en /api/water: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/api/air/all")
async def get_all_air_data():
    """Obtener TODOS los datos de sensores de aire sin límite de paginación"""
    if not redis_client or not redis_client.client:
        raise HTTPException(status_code=503, detail="Redis no disponible")
    
    try:
        devices = redis_client.get_active_devices('aire') or []
        
        result = []
        for device_name in devices:
            device_data = redis_client.get_device_data(device_name)
            # Puedes ajustar el límite del historial según necesites
            history = redis_client.get_device_history('aire', device_name, limit=100)
            
            result.append({
                **device_data,
                "recent_measurements": history,
                "device_name": device_name
            })
        
        return {
            "sensor_type": "air",
            "total_devices": len(devices),
            "data": result
        }
    except Exception as e:
        logger.error(f"Error en /api/air/all: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sound/all")
async def get_all_sound_data():
    """Obtener TODOS los datos de sensores de sonido sin límite"""
    if not redis_client or not redis_client.client:
        raise HTTPException(status_code=503, detail="Redis no disponible")
    
    try:
        devices = redis_client.get_active_devices('sonido') or []
        
        result = []
        for device_name in devices:  # Sin límite [:limit]
            device_data = redis_client.get_device_data(device_name)
            history = redis_client.get_device_history('sonido', device_name, limit=100)
            
            result.append({
                **device_data,
                "recent_measurements": history,
                "device_name": device_name
            })
        
        return {
            "sensor_type": "sound",
            "total_devices": len(devices),
            "data": result
        }
    except Exception as e:
        logger.error(f"Error en /api/sound/all: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/water/all")
async def get_all_water_data():
    """Obtener TODOS los datos de sensores de agua sin límite"""
    if not redis_client or not redis_client.client:
        raise HTTPException(status_code=503, detail="Redis no disponible")
    
    try:
        devices = redis_client.get_active_devices('agua') or []
        
        result = []
        for device_name in devices:  # Sin límite [:limit]
            device_data = redis_client.get_device_data(device_name)
            history = redis_client.get_device_history('agua', device_name, limit=100)
            
            result.append({
                **device_data,
                "recent_measurements": history,
                "device_name": device_name
            })
        
        return {
            "sensor_type": "water",
            "total_devices": len(devices),
            "data": result
        }
    except Exception as e:
        logger.error(f"Error en /api/water/all: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ENDPOINTS DE HISTORIAL ====================

@app.get("/api/{sensor_type}/history")
async def get_sensor_history(
    sensor_type: str,
    device_name: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    """
    Obtener historial de mediciones
    
    - sensor_type: air, sound, water
    - device_name: (opcional) filtrar por dispositivo
    - limit: número máximo de registros
    """
    if not redis_client or not redis_client.client:
        raise HTTPException(status_code=503, detail="Redis no disponible")
    
    # Mapear nombres en inglés a español (para Redis)
    sensor_map = {
        'air': 'aire',
        'sound': 'sonido',
        'water': 'agua'
    }
    
    if sensor_type not in sensor_map:
        raise HTTPException(status_code=400, detail="sensor_type must be: air, sound, or water")
    
    redis_sensor_type = sensor_map[sensor_type]
    
    try:
        if device_name:
            history = redis_client.get_device_history(redis_sensor_type, device_name, limit)
        else:
            history = redis_client.get_all_sensor_data(redis_sensor_type, limit)
        
        return {
            "sensor_type": sensor_type,
            "device_name": device_name,
            "total_records": len(history),
            "data": history
        }
    except Exception as e:
        logger.error(f"Error obteniendo historial: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ENDPOINTS DE MACHINE LEARNING ====================

@app.get("/api/stats/{sensor_type}")
async def get_sensor_stats(
    sensor_type: str,
    device_name: Optional[str] = None
):
    """
    Obtener estadísticas de un tipo de sensor
    """
    if not redis_client or not redis_client.client:
        raise HTTPException(status_code=503, detail="Redis no disponible")
    
    sensor_map = {
        'air': 'aire',
        'sound': 'sonido',
        'water': 'agua'
    }
    
    if sensor_type not in sensor_map:
        raise HTTPException(status_code=400, detail="sensor_type must be: air, sound, or water")
    
    redis_sensor_type = sensor_map[sensor_type]
    
    try:
        # Obtener datos
        if device_name:
            data = redis_client.get_device_history(redis_sensor_type, device_name, limit=1000)
        else:
            data = redis_client.get_all_sensor_data(redis_sensor_type, limit_per_device=100)
        
        if not data:
            return {"sensor_type": sensor_type, "stats": {}, "message": "No data available"}
        
        # Función para extraer valores
        def extract_values(records, field_name):
            values = []
            for record in records:
                try:
                    if 'data' in record and isinstance(record['data'], dict):
                        value = record['data'].get(field_name)
                        if value is not None:
                            if isinstance(value, str):
                                value = value.replace(',', '.').strip()
                            float_val = float(value)
                            # Verificar que no sea NaN
                            if float_val == float_val:  # Esto es True para números, False para NaN
                                values.append(float_val)
                except:
                    continue
            return values
        
        # Extraer valores según el tipo de sensor
        if sensor_type == 'air':
            values = extract_values(data, 'co2')
        elif sensor_type == 'sound':
            values = extract_values(data, 'laeq')
        elif sensor_type == 'water':
            values = extract_values(data, 'water_level')
        else:
            values = []
        
        if not values:
            return {"sensor_type": sensor_type, "stats": {}, "message": "No numeric data found"}
        
        # Calcular estadísticas manualmente (sin numpy para evitar problemas)
        stats = {
            "count": len(values),
            "mean": sum(values) / len(values) if values else 0,
            "min": min(values) if values else 0,
            "max": max(values) if values else 0,
        }
        
        # Calcular mediana, Q1 y Q3
        if values:
            sorted_values = sorted(values)
            n = len(sorted_values)
            stats["median"] = sorted_values[n//2] if n % 2 == 1 else (sorted_values[n//2-1] + sorted_values[n//2]) / 2
            stats["q1"] = sorted_values[n//4]
            stats["q3"] = sorted_values[3*n//4]
            
            # Calcular desviación estándar
            mean = stats["mean"]
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            stats["std"] = variance ** 0.5
        else:
            stats.update({"median": 0, "q1": 0, "q3": 0, "std": 0})
        
        return {
            "sensor_type": sensor_type,
            "device_name": device_name,
            "stats": stats,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/ml/train/{sensor_type}")
async def train_ml_model(sensor_type: str):
    """
    Entrenar modelo predictivo para un tipo de sensor
    
    - sensor_type: air, sound, water
    """
    if not ml_predictor:
        raise HTTPException(status_code=503, detail="ML Predictor no inicializado")
    
    sensor_map = {
        'air': 'aire',
        'sound': 'sonido', 
        'water': 'agua'
    }
    
    if sensor_type not in sensor_map:
        raise HTTPException(status_code=400, detail="sensor_type must be: air, sound, or water")
    
    redis_sensor_type = sensor_map[sensor_type]
    
    try:
        result = ml_predictor.train_model(redis_sensor_type)
        return result
    except Exception as e:
        logger.error(f"Error entrenando modelo: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ml/predict/{sensor_type}")
async def predict_future(
    sensor_type: str,
    days: int = Query(7, ge=1, le=30)
):
    """
    Predecir valores futuros
    
    - sensor_type: air, sound, water
    - days: días a predecir (1-30)
    """
    if not ml_predictor:
        raise HTTPException(status_code=503, detail="ML Predictor no inicializado")
    
    sensor_map = {
        'air': 'aire',
        'sound': 'sonido',
        'water': 'agua'
    }
    
    if sensor_type not in sensor_map:
        raise HTTPException(status_code=400, detail="sensor_type must be: air, sound, or water")
    
    redis_sensor_type = sensor_map[sensor_type]
    
    try:
        result = ml_predictor.predict(redis_sensor_type, days)
        return result
    except Exception as e:
        logger.error(f"Error haciendo predicción: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ml/info/{sensor_type}")
async def get_model_info(sensor_type: str):
    """
    Obtener información del modelo entrenado
    """
    if not ml_predictor:
        raise HTTPException(status_code=503, detail="ML Predictor no inicializado")
    
    sensor_map = {
        'air': 'aire',
        'sound': 'sonido',
        'water': 'agua'
    }
    
    if sensor_type not in sensor_map:
        raise HTTPException(status_code=400, detail="sensor_type must be: air, sound, or water")
    
    redis_sensor_type = sensor_map[sensor_type]
    
    try:
        info = ml_predictor.get_model_info(redis_sensor_type)
        return info
    except Exception as e:
        logger.error(f"Error obteniendo info del modelo: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== EJECUCIÓN ====================

if __name__ == "__main__":
    import uvicorn
    
    config = APIConfig()
    
    uvicorn.run(
        "app.main:app",
        host=config.host,
        port=config.port,
        reload=config.debug,
        log_level="info"
    )