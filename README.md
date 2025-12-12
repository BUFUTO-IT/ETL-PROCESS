# structure

iot-monitoring/
├── docker-compose.yml
├── .env.example
├── init-scripts/
│   ├── 01-init.sql
│   └── 02-permissions.sql
├── producer/
│   ├── main.py
│   ├── config.py
│   ├── data_loader.py
│   └── requirements.txt
├── consumer/
│   ├── main.py
│   ├── config.py
│   ├── models.py
│   ├── database.py
│   ├── etl_processor.py
│   └── requirements.txt
└── datasets/
    ├── aire.csv
    ├── sonido.csv
    └── agua.csv


markdown
# Sistema de Monitoreo IoT - Proceso ETL

## 📋 Descripción
Sistema completo de procesamiento ETL para datos de sensores IoT (aire, sonido, agua) utilizando RabbitMQ, PostgreSQL y Redis.

## 🏗️ Arquitectura
Producer (CSV) → RabbitMQ → Consumer → PostgreSQL + Redis

text

## 🚀 Instalación Rápida

### 1. Requisitos
- Docker y Docker Compose
- Python 3.11+ (para desarrollo local)
- Archivos CSV de datos en `/datasets/`

### 2. Configuración
```bash
# Clonar o copiar la estructura
chmod +x setup.sh
./setup.sh

# Editar variables de entorno (opcional)
cp .env.example .env
# Editar .env si es necesario
3. Ejecutar con Docker

bash
# Iniciar todos los servicios
docker-compose up -d

# Ver logs
docker-compose logs -f

# Detener servicios
docker-compose down
4. Ejecutar localmente (desarrollo)

bash
# Producer
cd producer
pip install -r requirements.txt
python main.py

# Consumer (en otra terminal)
cd consumer
pip install -r requirements.txt
python main.py
📊 Servicios

🐘 PostgreSQL

Puerto: 5432
Base de datos: iot_monitoring
Usuario: iot_user
Contraseña: iot_password
🐇 RabbitMQ

AMQP: 5672
Management UI: http://localhost:15672
Usuario: iot_user
Contraseña: iot_password
🟥 Redis

Puerto: 6379
Contraseña: iot_password
📈 Estructura de Datos

Tablas principales:

devices - Dispositivos registrados
air_measurements - Mediciones de calidad del aire
sound_measurements - Mediciones de ruido
water_measurements - Mediciones de nivel de agua
alerts - Alertas generadas
Vistas útiles:

device_status - Estado actual de dispositivos
🔍 Consultas útiles

sql
-- Dispositivos activos
SELECT * FROM devices WHERE is_active = true;

-- Últimas mediciones de aire
SELECT * FROM air_measurements 
ORDER BY timestamp DESC 
LIMIT 10;

-- Alertas no resueltas
SELECT * FROM alerts 
WHERE is_resolved = false 
ORDER BY timestamp DESC;
🛠️ Desarrollo

Agregar nuevo tipo de sensor:

Agregar cola en config.py
Crear método en ETLProcessor
Agregar modelo en models.py
Actualizar database.py
Variables de entorno importantes:

bash
# Producer
RABBITMQ_HOST=rabbitmq
BATCH_SIZE=50
SLEEP_INTERVAL=2.0

# Consumer
POSTGRES_HOST=postgres
REDIS_HOST=redis
LOG_LEVEL=INFO
📝 Logs

Los logs se guardan en consumer.log y producer.log
Niveles: DEBUG, INFO, WARNING, ERROR
Se muestran en consola y se guardan en archivo
🚨 Alertas automáticas

El sistema genera alertas para:

CO2 > 1000 ppm
Temperatura fuera de rango (10-30°C)
Ruido > 75 dB
Nivel de agua < 20%
Batería < 20%
🔗 Endpoints Redis

text
device:{device_name}          # Estado del dispositivo
history:{sensor}:{device}    # Historial (últimas 50)
active_devices:{sensor}      # Dispositivos activos
dashboard:air_quality        # Calidad del aire para dashboard
alert:{device}:{type}:latest # Última alerta
📊 Monitoreo

bash
# Ver logs en tiempo real
docker-compose logs -f consumer

# Estadísticas Redis
docker exec -it iot-redis redis-cli -a iot_password info stats

# Conectar a PostgreSQL
docker exec -it iot-postgres psql -U iot_user -d iot_monitoring
🐛 Troubleshooting

RabbitMQ no conecta:

bash
docker-compose restart rabbitmq
docker-compose logs rabbitmq
PostgreSQL sin conexión:

bash
docker-compose restart postgres
docker exec -it iot-postgres pg_isready -U iot_user
Redis sin respuesta:

bash
docker-compose restart redis
docker exec -it iot-redis redis-cli -a iot_password ping
📄 Licencia

MIT

text

---

## **✅ INSTRUCCIONES DE USO FINALES**

### 1. **Preparar el entorno:**
```bash
# Dar permisos al script de setup
chmod +x setup.sh

# Ejecutar setup
./setup.sh

# Colocar tus archivos CSV en la carpeta datasets/
# aire.csv, sonido.csv, agua.csv
2. Iniciar el sistema:

bash
# Levantar todos los servicios
docker-compose up -d

# Verificar que todo está funcionando
docker-compose ps

# Ver logs en tiempo real
docker-compose logs -f
3. Acceder a los servicios:

PostgreSQL: localhost:5432
Redis: localhost:6379
RabbitMQ Management: http://localhost:15672
Credenciales: iot_user / iot_password
4. Verificar datos:

bash
# Conectar a PostgreSQL
docker exec -it iot-postgres psql -U iot_user -d iot_monitoring

# Consultar datos
SELECT COUNT(*) FROM air_measurements;
SELECT COUNT(*) FROM sound_measurements;
SELECT COUNT(*) FROM water_measurements;

# Ver dispositivos
SELECT * FROM device_status;
5. Parar el sistema:

bash
# Detener y eliminar contenedores
docker-compose down

# Detener y eliminar contenedores y volúmenes
docker-compose down -v