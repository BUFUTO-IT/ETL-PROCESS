#!/bin/bash

echo "🔧 Instalación definitiva del ETL de sensores..."

# Instalar dependencias del sistema si es necesario
sudo apt update
sudo apt install -y python3-dev build-essential libssl-dev libffi-dev librdkafka-dev

# Crear/activar entorno virtual
python3 -m venv venv
source venv/bin/activate

# Actualizar pip
pip install --upgrade pip
pip install setuptools wheel

# requirements.txt actualizado
# cat > requirements.txt << EOF
# kafka-python==2.0.3
# pandas==2.2.0
# numpy==1.26.4
# python-dotenv==1.0.0
# confluent-kafka==2.2.0
# pyarrow==14.0.1
# EOF

# Instalar dependencias
pip install -r requirements.txt

# Verificación completa
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

print('🎉 ¡Sistema listo!')
"

echo "✨ ¡Instalación completada!"