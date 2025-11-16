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
HOME_DIR=$(eval echo "~$NORMAL_USER")
echo "👤 Usuario normal: $NORMAL_USER"

# Crear directorios necesarios
echo "📁 Creando directorios..."
mkdir -p logs
mkdir -p processed_data
mkdir -p $HOME_DIR/Desktop/ETL-KAFKA/logs 2>/dev/null || true

# Crear entorno virtual como usuario normal
echo "🐍 Creando entorno virtual..."
sudo -u $NORMAL_USER python3 -m venv venv

# Instalar dependencias como usuario normal
echo "📦 Instalando dependencias Python..."
sudo -u $NORMAL_USER bash -c "source venv/bin/activate && pip install -r requirements.txt"

# Configurar permisos
echo "🔒 Configurando permisos..."
chown -R $NORMAL_USER:$NORMAL_USER venv/
chown -R $NORMAL_USER:$NORMAL_USER logs/
chown -R $NORMAL_USER:$NORMAL_USER processed_data/

# Kafka con Docker (usa sudo para docker)
echo "🐳 Iniciando Kafka..."
docker compose down 2>/dev/null
docker compose up -d

# Esperar a que Kafka esté listo
echo "⏳ Esperando a que Kafka esté listo..."
sleep 20

# Crear topics
echo "📊 Creando topics..."
docker exec etl-kafka-1 kafka-topics --create --topic sensor-air-quality --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null || echo "✅ Topic air-quality ya existe"
docker exec etl-kafka-1 kafka-topics --create --topic sensor-sound --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null || echo "✅ Topic sound ya existe"
docker exec etl-kafka-1 kafka-topics --create --topic sensor-water --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null || echo "✅ Topic water ya existe"

# Verificar topics
echo "📋 Topics disponibles:"
docker exec etl-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "✅ Setup completado con sudo"
echo ""
echo "📝 Ahora ejecuta SIN sudo:"
echo "   source venv/bin/activate"
echo "   python start_etl_fixed.py"