FROM python:3.10

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 🔧 Crear carpetas necesarias con permisos
RUN mkdir -p /app/logs /app/data_warehouse /app/processed_data \
    && chmod -R 777 /app/logs /app/data_warehouse /app/processed_data

CMD ["python", "start_etl_fixed.py"]
