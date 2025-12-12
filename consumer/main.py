# consumer/main.py
import pika
import json
import time
import logging
from datetime import datetime
from config import RabbitMQConfig, DatabaseConfig, ConsumerConfig
from database import DatabaseManager
from etl_processor import ETLProcessor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('consumer.log')
    ]
)
logger = logging.getLogger(__name__)

class Consumer:
    """Consumidor que procesa mensajes de RabbitMQ"""
    # Configuración
    rabbit_config = RabbitMQConfig()
    db_config = DatabaseConfig()

    def __init__(self, rabbit_config: RabbitMQConfig, db_config: DatabaseConfig):
        self.rabbit_config = rabbit_config
        self.db_config = db_config
        self.db_manager = None
        self.connection = None
        self.channel = None
        self.processed_count = 0
        self.error_count = 0
        
    def connect_rabbitmq(self):
        """Conectar a RabbitMQ con reintentos"""
        max_retries = 5
        retry_delay = 5
        

        
        for attempt in range(max_retries):
            try:
                logger.info(f"Conectando a RabbitMQ (intento {attempt + 1}/{max_retries})...")
                
                credentials = pika.PlainCredentials(
                    self.rabbit_config.username,
                    self.rabbit_config.password
                )
                
                parameters = pika.ConnectionParameters(
                    host=self.rabbit_config.host,
                    port=self.rabbit_config.port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                    connection_attempts=3,
                    retry_delay=3
                )
                
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Declarar colas
                for queue_name in self.rabbit_config.queue_names.values():
                    self.channel.queue_declare(
                        queue=queue_name,
                        durable=True,
                        arguments={
                            'x-message-ttl': 86400000  # 24 horas en ms - ¡IGUAL QUE PRODUCER!
                        }
                    )

                
                # QoS: procesar un mensaje a la vez
                self.channel.basic_qos(prefetch_count=1)
                
                logger.info("✅ Conectado a RabbitMQ exitosamente")
                return True
                
            except Exception as e:
                logger.error(f"Error conectando a RabbitMQ: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Reintentando en {retry_delay} segundos...")
                    time.sleep(retry_delay)
                else:
                    logger.error("❌ No se pudo conectar a RabbitMQ después de varios intentos")
                    return False
    
    def connect_databases(self):
        """Conectar a bases de datos"""
        logger.info("Conectando a bases de datos...")
        
        self.db_manager = DatabaseManager(self.db_config)
        
        postgres_ok = self.db_manager.connect_postgres()
        if not postgres_ok:
            logger.error("❌ No se pudo conectar a PostgreSQL")
            return False
        
        redis_ok = self.db_manager.connect_redis()
        if not redis_ok:
            logger.error("❌ No se pudo conectar a Redis")
            return False
        
        logger.info("✅ Conectado a todas las bases de datos")
        return True
    
    def process_message(self, ch, method, properties, body):
        """Procesar un mensaje recibido de RabbitMQ"""
        message_id = None
        sensor_type = None
        
        try:
            # Decodificar mensaje
            message = json.loads(body.decode())
            message_id = message.get('message_id', 'unknown')
            sensor_type = message.get('sensor_type')
            data = message.get('data', {})
            produced_at = message.get('produced_at')
            
            logger.info(f"📥 Procesando mensaje {message_id} - Sensor: {sensor_type} - Dispositivo: {data.get('device_name', 'Unknown')}")
            
            # Validar tipo de sensor
            if sensor_type not in ['aire', 'sonido', 'agua']:
                logger.error(f"Tipo de sensor desconocido: {sensor_type}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Aplicar ETL según tipo de sensor
            if sensor_type == 'aire':
                processed_data = ETLProcessor.process_air_data(data)
            elif sensor_type == 'sonido':
                processed_data = ETLProcessor.process_sound_data(data)
            elif sensor_type == 'agua':
                processed_data = ETLProcessor.process_water_data(data)
            else:
                logger.error(f"Tipo de sensor no implementado: {sensor_type}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Agregar características temporales
            time_features = ETLProcessor.add_time_features(processed_data.get('timestamp'))
            processed_data.update(time_features)
            
            # Convertir timestamp string a datetime
            if processed_data.get('timestamp'):
                try:
                    if isinstance(processed_data['timestamp'], str):
                        processed_data['timestamp'] = datetime.fromisoformat(
                            processed_data['timestamp'].replace('Z', '+00:00')
                        )
                except Exception as e:
                    logger.warning(f"Error convirtiendo timestamp: {e}")
                    processed_data['timestamp'] = datetime.utcnow()
            else:
                processed_data['timestamp'] = datetime.utcnow()
            
            # Validar que haya datos mínimos
            if not processed_data.get('device_name'):
                logger.warning("Mensaje sin nombre de dispositivo, descartando...")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Guardar en bases de datos
            postgres_id = self.db_manager.save_to_postgres(sensor_type, processed_data)
            redis_ok = self.db_manager.save_to_redis(sensor_type, processed_data)
            
            # Estadísticas
            self.processed_count += 1
            
            if postgres_id:
                logger.info(f"💾 Guardado en PostgreSQL (ID: {postgres_id})")
            else:
                logger.warning("No se pudo guardar en PostgreSQL")
                self.error_count += 1
            
            if redis_ok:
                logger.debug("Caché actualizado en Redis")
            else:
                logger.warning("No se pudo guardar en Redis")
                self.error_count += 1
            
            # Log cada 100 mensajes procesados
            if self.processed_count % 100 == 0:
                logger.info(f"📊 Estadísticas: {self.processed_count} mensajes procesados, {self.error_count} errores")
            
            # Confirmar procesamiento del mensaje
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.debug(f"✅ Mensaje {message_id} procesado exitosamente")
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error decodificando JSON: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            self.error_count += 1
            
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje {message_id}: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            self.error_count += 1
    
    def start_consuming(self):
        """Iniciar consumo de mensajes"""
        logger.info("🚀 Iniciando consumidor IoT...")
        
        try:
            # Configurar callbacks para cada cola
            for sensor_type, queue_name in self.rabbit_config.queue_names.items():
                self.channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=self.process_message,
                    auto_ack=False
                )
                logger.info(f"👂 Escuchando cola: {queue_name}")
            
            logger.info("✅ Consumidor listo. Esperando mensajes...")
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("🛑 Deteniendo consumidor...")
            self.channel.stop_consuming()
        except Exception as e:
            logger.error(f"💥 Error en el consumidor: {e}")
        finally:
            self.close()
    
    def close(self):
        """Cerrar conexiones de manera segura"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("🔌 Conexión RabbitMQ cerrada")
        except Exception as e:
            logger.error(f"Error cerrando RabbitMQ: {e}")
        
        try:
            if self.db_manager:
                self.db_manager.close()
        except Exception as e:
            logger.error(f"Error cerrando bases de datos: {e}")
        
        # Resumen final
        logger.info(f"📊 RESUMEN FINAL: {self.processed_count} mensajes procesados, {self.error_count} errores")

def main():
    """Función principal del consumidor"""
    try:
        # Configuración
        rabbit_config = RabbitMQConfig()
        db_config = DatabaseConfig()
                
        # Crear consumidor
        consumer = Consumer(rabbit_config, db_config)
        
        logger.info("=" * 50)
        logger.info("CONSUMIDOR IoT - SISTEMA DE MONITOREO")
        logger.info("=" * 50)
        
        # Conectar a RabbitMQ
        if not consumer.connect_rabbitmq():
            logger.error("No se pudo conectar a RabbitMQ")
            return
        
        # Conectar a bases de datos
        if not consumer.connect_databases():
            logger.error("No se pudo conectar a las bases de datos")
            consumer.close()
            return
        
        # Iniciar consumo
        consumer.start_consuming()
        
    except Exception as e:
        logger.error(f"Error en main: {e}")

if __name__ == "__main__":
    main()