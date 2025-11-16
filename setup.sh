#!/bin/bash

echo "🔧 Setup ETL Kafka - Modo Docker con Sudo"

# Verificar si estamos usando sudo
if [ "$EUID" -ne 0 ]; then
    echo "❌ Este script debe ejecutarse con sudo para Docker"
    echo "   Ejecuta: sudo bash setup.sh"
    exit 1
fi

# Obtener el usuario normal
NORMAL_USER=$(logname)
echo "👤 Usuario normal: $NORMAL_USER"

# Crear directorios necesarios
echo "📁 Creando directorios..."
mkdir -p logs processed_data data_warehouse

# Crear entorno virtual como usuario normal
echo "🐍 Creando entorno virtual..."
sudo -u $NORMAL_USER python3 -m venv venv

# Instalar dependencias como usuario normal
echo "📦 Instalando dependencias Python..."
sudo -u $NORMAL_USER bash -c "source venv/bin/activate && pip install -r requirements.txt"

# Configurar permisos
echo "🔒 Configurando permisos..."
chown -R $NORMAL_USER:$NORMAL_USER venv/ logs/ processed_data/ data_warehouse/

# Kafka con Docker
echo "🐳 Iniciando Kafka..."
docker compose down 2>/dev/null
docker compose up -d

# Esperar a que Kafka esté listo
echo "⏳ Esperando a que Kafka esté listo (30 segundos)..."
sleep 30

# Obtener el nombre REAL del contenedor de Kafka
KAFKA_CONTAINER=$(docker ps --filter "name=kafka" --format "{{.Names}}" | grep kafka | head -1)

if [ -z "$KAFKA_CONTAINER" ]; then
    echo "❌ No se pudo encontrar el contenedor de Kafka"
    echo "📋 Contenedores running:"
    docker ps --format "table {{.Names}}\t{{.Status}}"
    echo "🔄 Intentando reiniciar Kafka..."
    docker compose restart kafka
    sleep 15
    KAFKA_CONTAINER=$(docker ps --filter "name=kafka" --format "{{.Names}}" | grep kafka | head -1)
fi

if [ -z "$KAFKA_CONTAINER" ]; then
    echo "❌ Kafka no está corriendo después del reinicio"
    echo "📋 Logs de Kafka:"
    docker compose logs kafka
    exit 1
fi

echo "🔍 Contenedor de Kafka detectado: $KAFKA_CONTAINER"

# Crear topics
echo "📊 Creando topics..."
for topic in "sensor-air-quality" "sensor-sound" "sensor-water"; do
    if docker exec $KAFKA_CONTAINER kafka-topics --create --topic $topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null; then
        echo "✅ Topic $topic creado"
    else
        echo "✅ Topic $topic ya existe"
    fi
done

# Verificar topics
echo "📋 Topics disponibles:"
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "✅ Setup completado con sudo"
echo ""
echo "📝 Ahora ejecuta SIN sudo:"
echo "   source venv/bin/activate"
echo "   python kafka_producer_fixed.py"
echo ""
echo "🌐 Kafka UI disponible en: http://localhost:8080"