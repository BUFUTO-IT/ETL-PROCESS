#!/usr/bin/env python3
"""
Script de inicio rápido para el ETL de sensores - VERSIÓN CORREGIDA
"""

import time
import logging
from kafka_producer_fixed import SensorDataProducer
from kafka_consumer_etl_fixed import SensorDataETL
import threading
import os

# Configurar logging simple
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def verify_files_exist():
    """Verifica que los archivos CSV existan"""
    required_files = ['co2.csv', 'sound.csv', 'water.csv']
    missing_files = []
    
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        logger.warning(f"⚠️ Archivos faltantes: {missing_files}")
        logger.info("💡 Asegúrate de tener los archivos CSV en el directorio actual")
        return False
    
    return True

def run_etl_pipeline():
    """Ejecuta el pipeline ETL completo"""
    
    def start_consumer():
        """Inicia el consumidor en un hilo separado"""
        try:
            logger.info("🔄 Iniciando consumidor ETL...")
            consumer = SensorDataETL()
            consumer.start_processing()
        except Exception as e:
            logger.error(f"❌ Error en consumidor: {e}")
    
    def start_producer():
        """Inicia el productor después de una breve espera"""
        time.sleep(5)  # Esperar a que el consumidor esté listo
        try:
            logger.info("🚀 Iniciando productor...")
            producer = SensorDataProducer()
            
            # Procesar archivos CSV
            files = ['co2.csv', 'sound.csv', 'water.csv']
            for csv_file in files:
                if os.path.exists(csv_file):
                    producer.stream_csv_data(csv_file, delay=0.05)
                else:
                    logger.warning(f"⚠️ Archivo no encontrado: {csv_file}")
            
            producer.close()
            logger.info("✅ Productor completado")
            
        except Exception as e:
            logger.error(f"❌ Error en productor: {e}")
    
    # Verificar archivos primero
    if not verify_files_exist():
        logger.error("❌ No se pueden encontrar todos los archivos CSV necesarios")
        return
    
    # Iniciar consumidor en hilo separado
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)
    consumer_thread.start()
    
    # Iniciar productor en el hilo principal
    start_producer()

if __name__ == "__main__":
    logger.info("🧪 INICIANDO PIPELINE ETL CORREGIDO Y MEJORADO")
    logger.info("📁 Se crearán archivos JSON en la carpeta 'data_warehouse'")
    
    run_etl_pipeline()