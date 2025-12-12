# producer/main.py
import pika
import json
import time
import logging
from datetime import datetime
from config import RabbitMQConfig, ProducerConfig
from data_loader import DataLoader

from state_manager import ProducerStateManager

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Producer:
    """Productor que envía datos a RabbitMQ"""
    
    def __init__(self, rabbit_config: RabbitMQConfig):
        self.config = rabbit_config
        self.connection = None
        self.channel = None
        self.data_loader = DataLoader()
        
    def connect(self):
        """Conectar a RabbitMQ con reintentos"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Conectando a RabbitMQ (intento {attempt + 1}/{max_retries})...")
                
                credentials = pika.PlainCredentials(
                    self.config.username,
                    self.config.password
                )
                
                parameters = pika.ConnectionParameters(
                    host=self.config.host,
                    port=self.config.port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                    connection_attempts=3,
                    retry_delay=3
                )
                
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Declarar colas duraderas
                for queue_name in self.config.queue_names.values():
                    self.channel.queue_declare(
                        queue=queue_name,
                        durable=True,
                        arguments={
                            'x-message-ttl': 86400000  # 24 horas en ms
                        }
                    )
                
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
    
    def send_message(self, sensor_type, data):
        """Enviar mensaje a la cola correspondiente"""
        try:
            queue_name = self.config.queue_names.get(sensor_type)
            if not queue_name:
                logger.error(f"Tipo de sensor no válido: {sensor_type}")
                return False
            
            # Crear mensaje estructurado
            message = {
                'sensor_type': sensor_type,
                'data': data,
                'produced_at': datetime.utcnow().isoformat(),
                'message_id': f"{sensor_type}_{datetime.utcnow().timestamp()}"
            }
            
            # Publicar mensaje
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message, default=str),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistente
                    content_type='application/json',
                    timestamp=int(time.time())
                )
            )
            
            logger.debug(f"📤 Mensaje enviado a {queue_name}: {data.get('device_name', 'Unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando mensaje: {e}")
            return False
    
    def start_producing(self, producer_config: ProducerConfig):
        """Iniciar producción de datos"""
        logger.info("🚀 Iniciando productor de datos IoT...")
        logger.info(f"Configuración: batch_size={producer_config.batch_size}, sleep={producer_config.sleep_interval}")
        
        # AÑADIR: Guardar config como atributo
        self.producer_config = producer_config
        
        logger.info(f"Configuración: batch_size={self.producer_config.batch_size}, sleep={self.producer_config.sleep_interval}")

        sensor_types = ['aire', 'sonido', 'agua']
        iteration = 0
        
        try:
            while True:
                iteration += 1
                logger.info(f"--- Iteración #{iteration} ---")
                
                for sensor_type in sensor_types:
                    # Obtener lote de datos
                    batch = self.data_loader.get_batch(
                        sensor_type, 
                        producer_config.batch_size
                    )
                    
                    if not batch:
                        logger.warning(f"No hay datos para {sensor_type}")
                        continue
                    
                    # Enviar cada registro
                    success_count = 0
                    for record in batch:
                        if self.send_message(sensor_type, record):
                            success_count += 1
                    
                    if success_count > 0:
                        logger.info(f"✅ Enviados {success_count}/{len(batch)} registros de {sensor_type}")
                    else:
                        logger.warning(f"⚠️ No se enviaron registros de {sensor_type}")
                
                # Estadísticas
                logger.info(f"Esperando {producer_config.sleep_interval} segundos...")
                time.sleep(producer_config.sleep_interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Deteniendo productor...")
        except Exception as e:
            logger.error(f"💥 Error crítico en el productor: {e}")
        finally:
            self.close()
    
    def close(self):
        """Cerrar conexión de manera segura"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("🔌 Conexión RabbitMQ cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")
        
    def start_producing_complete(self, producer_config: ProducerConfig):
        """Enviar TODOS los datos de los datasets (una sola vez)"""
        logger.info("🚀 Iniciando ENVÍO COMPLETO de datos IoT...")
        
        sensor_types = ['aire', 'sonido', 'agua']
        total_sent = 0
        
        try:
            for sensor_type in sensor_types:
                logger.info(f"📤 Enviando datos de {sensor_type}...")
                
                # Obtener TODOS los datos ordenados
                all_data = self.data_loader.get_all_data_ordered(sensor_type)
                
                if not all_data:
                    logger.warning(f"No hay datos para {sensor_type}")
                    continue
                
                logger.info(f"📊 Total de registros {sensor_type}: {len(all_data)}")
                
                # Enviar en lotes para no sobrecargar RabbitMQ
                batch_size = 100  # Tamaño de lote fijo para envío completo
                for i in range(0, len(all_data), batch_size):
                    batch = all_data[i:i + batch_size]
                    
                    for record in batch:
                        if self.send_message(sensor_type, record):
                            total_sent += 1
                    
                    # Log de progreso
                    progress = min(i + batch_size, len(all_data))
                    logger.info(f"↳ Progreso {sensor_type}: {progress}/{len(all_data)} ({progress/len(all_data)*100:.1f}%)")
                    
                    # Pequeña pausa entre lotes
                    time.sleep(0.1)
                
                logger.info(f"✅ Datos de {sensor_type} enviados: {len(all_data)} registros")
            
            logger.info(f"🎉 ENVÍO COMPLETO: {total_sent} registros enviados en total")
            return True
            
        except Exception as e:
            logger.error(f"💥 Error en envío completo: {e}")
            return False
        
# main.py - Producer Inteligente
class SmartProducer(Producer):
    def start_smart_producing(self, producer_config: ProducerConfig):
        """Productor que envía todos los datos progresivamente"""
        logger.info("🤖 Iniciando productor INTELIGENTE...")
        
        # Inicializar gestor de estado
        state_manager = ProducerStateManager()
        
        sensor_types = ['aire', 'sonido', 'agua']
        
        try:
            while True:
                iteration_sent = 0
                
                for sensor_type in sensor_types:
                    # Obtener estado actual
                    state = state_manager.get_sensor_state(sensor_type)
                    start_index = state['last_index']
                    
                    # Cargar todos los datos
                    all_data = self.data_loader.load_and_process_data(sensor_type)
                    
                    if not all_data:
                        continue
                    
                    # Ordenar por timestamp
                    all_data.sort(key=lambda x: x.get('timestamp', ''))
                    
                    # Determinar cuántos datos enviar en esta iteración
                    batch_size = min(producer_config.batch_size, len(all_data) - start_index)
                    
                    if batch_size <= 0:
                        # Ya se enviaron todos, reiniciar o saltar
                        if start_index >= len(all_data):
                            logger.info(f"✅ {sensor_type}: Todos los datos enviados")
                            continue
                        batch_size = producer_config.batch_size
                        start_index = 0
                    
                    # Obtener lote ordenado
                    end_index = start_index + batch_size
                    batch = all_data[start_index:end_index]
                    
                    # Enviar lote
                    sent_count = 0
                    for record in batch:
                        if self.send_message(sensor_type, record):
                            sent_count += 1
                    
                    # Actualizar estado
                    state_manager.update_sensor_state(
                        sensor_type, 
                        end_index, 
                        sent_count
                    )
                    
                    iteration_sent += sent_count
                    
                    # Log detallado
                    logger.info(
                        f"📤 {sensor_type}: Enviados {sent_count} registros "
                        f"(Total: {state['total_sent'] + sent_count}/{len(all_data)})"
                    )
                
                if iteration_sent == 0:
                    logger.info("🎉 ¡TODOS los datos han sido enviados!")
                    logger.info("🔄 Reiniciando ciclo...")
                    # Opcional: resetear estado para comenzar de nuevo
                    for sensor_type in sensor_types:
                        state_manager.update_sensor_state(sensor_type, 0, 0)
                
                logger.info(f"⏳ Esperando {producer_config.sleep_interval} segundos...")
                time.sleep(producer_config.sleep_interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Productor detenido")
        finally:
            self.close()

import argparse


def main():
    """Función principal del productor"""
    try:
        # Configurar argumentos de línea de comandos
        parser = argparse.ArgumentParser(description='Productor de datos IoT')
        parser.add_argument('--mode', choices=['random', 'complete', 'smart'], 
                          default='random', help='Modo de envío: random (aleatorio), complete (completo una vez), smart (inteligente)')
        args = parser.parse_args()
        
        # Configuración
        rabbit_config = RabbitMQConfig()
        producer_config = ProducerConfig()
        
        # Crear productor según el modo
        if args.mode == 'smart':
            producer = SmartProducer(rabbit_config)
        else:
            producer = Producer(rabbit_config)
        
        # Conectar
        if producer.connect():
            # Iniciar producción según el modo
            if args.mode == 'complete':
                producer.start_producing_complete(producer_config)
            elif args.mode == 'smart':
                producer.start_smart_producing(producer_config)
            else:  # random (por defecto)
                producer.start_producing(producer_config)
        else:
            logger.error("No se pudo iniciar el productor")
            
    except Exception as e:
        logger.error(f"Error en main: {e}")

if __name__ == "__main__":
    main()