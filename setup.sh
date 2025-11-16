#!/bin/bash

echo "🚀 INICIALIZACIÓN COMPLETA DEL SISTEMA ETL DE SENSORES"
echo "======================================================="

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para imprimir con color
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Verificar si estamos en Linux (para instalación de paquetes)
check_linux() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        return 0
    else
        return 1
    fi
}

# ============================================================================
# 🐍 FASE 1: VERIFICACIÓN DE DEPENDENCIAS DEL SISTEMA
# ============================================================================

print_status "FASE 1: Verificando dependencias del sistema..."

# Verificar Python
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")
    print_success "Python $PYTHON_VERSION detectado"
else
    print_error "Python 3 no encontrado. Por favor instala Python 3.8 o superior"
    exit 1
fi

# Verificar Docker
if command -v docker &> /dev/null; then
    print_success "Docker detectado"
else
    print_error "Docker no encontrado. Por favor instala Docker"
    echo "Visita: https://docs.docker.com/get-docker/"
    exit 1
fi

# Verificar Docker Compose
if command -v docker-compose &> /dev/null || docker compose version &> /dev/null; then
    print_success "Docker Compose detectado"
else
    print_error "Docker Compose no encontrado"
    exit 1
fi

# Verificar Git
if command -v git &> /dev/null; then
    print_success "Git detectado"
else
    print_warning "Git no encontrado (opcional para este script)"
fi

# ============================================================================
# 📦 FASE 2: INSTALACIÓN DE DEPENDENCIAS DEL SISTEMA (Linux)
# ============================================================================

if check_linux; then
    print_status "FASE 2: Instalando dependencias del sistema (Linux)..."
    
    # Actualizar repositorios
    sudo apt update
    
    # Instalar dependencias del sistema
    sudo apt install -y python3-dev build-essential libssl-dev libffi-dev librdkafka-dev
    
    print_success "Dependencias del sistema instaladas"
else
    print_warning "Sistema no-Linux detectado. Saltando instalación de paquetes del sistema."
    print_warning "Asegúrate de tener instalado: python3-dev, build-essential, libssl-dev, libffi-dev, librdkafka-dev"
fi

# ============================================================================
# 🐍 FASE 3: CONFIGURACIÓN DEL ENTORNO PYTHON
# ============================================================================

print_status "FASE 3: Configurando entorno Python..."

# Crear entorno virtual
if [ ! -d "venv" ]; then
    python3 -m venv venv
    print_success "Entorno virtual creado"
else
    print_warning "El entorno virtual 'venv' ya existe"
fi

# Activar entorno virtual
print_status "Activando entorno virtual..."
source venv/bin/activate

# Actualizar pip
print_status "Actualizando pip..."
pip install --upgrade pip
pip install setuptools wheel

# Instalar dependencias Python
if [ -f "requirements.txt" ]; then
    print_status "Instalando dependencias Python..."
    pip install -r requirements.txt
else
    print_error "Archivo requirements.txt no encontrado"
    exit 1
fi

# ============================================================================
# ✅ FASE 4: VERIFICACIÓN DE LA INSTALACIÓN
# ============================================================================

print_status "FASE 4: Verificando instalación..."

python -c "
import sys
print('🐍 Python:', sys.version)

try:
    import kafka
    print(f'✅ kafka-python: {kafka.__version__}')
except Exception as e:
    print(f'❌ kafka-python: {e}')

try:
    import pandas as pd
    print(f'✅ pandas: {pd.__version__}')
except Exception as e:
    print(f'❌ pandas: {e}')

try:
    import numpy as np
    print(f'✅ numpy: {np.__version__}')
except Exception as e:
    print(f'❌ numpy: {e}')

try:
    import confluent_kafka
    print(f'✅ confluent-kafka: {confluent_kafka.__version__}')
except Exception as e:
    print(f'❌ confluent-kafka: {e}')

try:
    import pyarrow
    print(f'✅ pyarrow: {pyarrow.__version__}')
except Exception as e:
    print(f'❌ pyarrow: {e}')

print('🎉 ¡Todas las dependencias verificadas!')
"

# ============================================================================
# 🐳 FASE 5: CONFIGURACIÓN DE KAFKA CON DOCKER
# ============================================================================

print_status "FASE 5: Configurando Kafka con Docker..."

# Iniciar Kafka con Docker
print_status "Iniciando contenedores de Kafka..."
docker compose up -d

# Esperar a que Kafka esté listo
print_status "Esperando a que Kafka esté listo (10 segundos)..."
sleep 10

# Crear topics
print_status "Creando topics de Kafka..."

# Función para crear topic con reintentos
create_topic() {
    local topic=$1
    local max_attempts=3
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        print_status "Intentando crear topic $topic (intento $attempt/$max_attempts)..."
        
        if docker exec -i etl-kafka-1 kafka-topics --create --topic $topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null; then
            print_success "Topic $topic creado exitosamente"
            return 0
        else
            print_warning "Error creando topic $topic, reintentando..."
            sleep 5
            ((attempt++))
        fi
    done
    
    print_error "No se pudo crear el topic $topic después de $max_attempts intentos"
    return 1
}

# Crear todos los topics
create_topic "sensor-air-quality"
create_topic "sensor-sound" 
create_topic "sensor-water"

# Verificar topics creados
print_status "Listando topics existentes..."
docker exec -i etl-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# ============================================================================
# 🎉 FASE 6: COMPROBACIÓN FINAL
# ============================================================================

print_status "FASE 6: Comprobación final del sistema..."

# Verificar que los contenedores estén corriendo
if docker ps | grep -q "kafka"; then
    print_success "Contenedores de Kafka están en ejecución"
else
    print_error "Los contenedores de Kafka no están corriendo"
fi

# Verificar archivos esenciales
essential_files=("config.py" "schemas.py" "validation.py" "data_warehouse.py")
missing_files=()

for file in "${essential_files[@]}"; do
    if [ ! -f "$file" ]; then
        missing_files+=("$file")
    fi
done

if [ ${#missing_files[@]} -eq 0 ]; then
    print_success "Todos los archivos esenciales encontrados"
else
    print_warning "Archivos faltantes: ${missing_files[*]}"
fi

echo
echo "======================================================="
print_success "🎉 CONFIGURACIÓN COMPLETADA EXITOSAMENTE!"
echo "======================================================="
echo
echo "📝 PRÓXIMOS PASOS:"
echo "1. Activa el entorno virtual: source venv/bin/activate"
echo "2. Verifica que tengas los archivos CSV: co2.csv, sound.csv, water.csv"
echo "3. Ejecuta el ETL: python start_etl_fixed.py"
echo
echo "🐳 Comandos útiles de Docker:"
echo "   - Ver logs: docker compose logs -f"
echo "   - Detener: docker compose down"
echo "   - Reiniciar: docker compose restart"
echo
echo "📊 Para probar el sistema:"
echo "   python kafka_producer_fixed.py --delay 0.01"
echo "   python kafka_consumer_etl_fixed.py"
echo