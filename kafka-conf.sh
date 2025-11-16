# Iniciar Kafka con Docker
docker compose up -d

# Crear topics manualmente
docker exec -it etl-kafka-1 kafka-topics --create --topic sensor-air-quality --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it etl-kafka-1 kafka-topics --create --topic sensor-sound --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it etl-kafka-1 kafka-topics --create --topic sensor-water --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Ver topics
docker exec -it etl-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Detener
#docker compose down