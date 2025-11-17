# 🚀 Sistema ETL para Datos de Sensores en Tiempo Real

Sistema completo de procesamiento ETL (Extract, Transform, Load) para datos de sensores IoT utilizando Apache Kafka y Python.

## 📋 Requisitos del Sistema

### 🔧 **Requisitos Obligatorios**

- **Python 3.8+** 
  - ✅ **No requiere Java** - Kafka corre en contenedores Docker
  - Descarga: https://www.python.org/downloads/

- **Docker & Docker Compose**
  - Docker Desktop: https://www.docker.com/products/docker-desktop/
  - Docker Engine: https://docs.docker.com/engine/install/

- **Sistema Operativo**: Windows 10+, macOS 10.15+, o Linux (Ubuntu 18.04+)

### 📦 **Requisitos Opcionales**

- **Git** (para control de versiones)
- **4GB+ RAM** libres para Docker
- **5GB+ espacio** en disco

## 🚀 Instalación Rápida

### Paso 1: Clonar/Descargar el Proyecto
```bash
# Si usas Git
git clone https://github.com/BUFUTO-IT/ETL-PROCESS.git
cd ETL-PROCESS


# Correr el archivo setup.sh
# Linux
source setup.sh
# Windows
setup.sh

# Correr Consumer una terminal
python kafka_consumer_etl_fixed.py

# Correr Producer otra terminal
python kafka_producer_fixed.py
# O descarga manualmente los archivos