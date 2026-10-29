# Como ejecutar el pipeline en local (Semana 2)

Requisitos: Docker Desktop abierto, Python 3.13 y Git Bash (terminal de VS Code).
Usa tres terminales: una para Spark, una para el producer y una para comandos.

## 1. Levantar Kafka
    docker compose up -d
    docker compose ps

## 2. Crear el topic con 3 particiones (una sola vez por cada `docker compose up`)
    MSYS_NO_PATHCONV=1 docker exec rapidgo-v8-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic rapidgo.flota.v8 --partitions 3 --replication-factor 1
    MSYS_NO_PATHCONV=1 docker exec rapidgo-v8-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic rapidgo.flota.v8

## 3. Spark Streaming (terminal 1)
    docker compose run --rm spark

Prueba rapida con ventanas cortas y salida en pantalla:
    SALIDA=consola VENTANA="1 minute" WATERMARK="30 seconds" docker compose run --rm spark

## 4. Producer (terminal 2)
    python -m venv .venv
    source .venv/Scripts/activate
    pip install -r producer/requirements.txt
    python producer/producer_flota.py --intervalo 10 --duracion 600

## 5. Verificar el Data Lake (terminal 3)
    docker compose run --rm verificar

## 6. Apagar y limpiar
    docker compose down
    rm -rf datalake/raw datalake/processed datalake/curated datalake/_checkpoints
