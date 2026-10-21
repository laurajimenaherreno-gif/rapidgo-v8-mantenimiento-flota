# Checkpoint Semana 1
## 1. Qué quedó funcionando esta semana
Dejamos listo el repositorio con los tres integrantes como colaboradores, el tablero Kanban con sus cinco columnas y 15 Issues repartidos en las cuatro semanas, y la estructura de carpetas con el README. También definimos el contrato del evento de flota, 3 KPIs, 2 alertas, el diagrama de arquitectura y un `docker-compose.yml` preliminar con Kafka.

## 2. Evidencia real observada
Esta semana se centró en el diseño de la arquitectura y en el montaje general del repositorio y las carpetas, así que todavía no hay resultados de ejecución como logs, offsets ni tiempos. Esos datos los emepezaremos a tener desde la Semana 2, lo que sí se puede comprobar en el repositorio por ahora es que:
- Creamos los 15 Issues antes de abrir cualquier Pull Request. Por eso el primer PR (estructura inicial y README) quedó con el número #16, ya que los Issues y los PR comparten la misma numeración.
- La estructura de carpetas que pide la guía (`airflow/dags`, `producer`, `spark`, `tests`, `samples`, `datalake`, `docs` y `.github/workflows`) ya está en `main`.
- El diagrama de arquitectura quedó guardado en `docs/arquitectura.png`, el contrato del evento en `docs/contrato-evento.md` y los KPIs y alertas en `docs/kpis-y-alertas.md`.

## 3. Decisión técnica más importante
Elegimos `vehiculo_id` como key del topic `rapidgo.flota.v8`, con 3 particiones. Así las lecturas de un mismo vehículo van siempre a la misma partición y no se desordenan, pues si se desordenan, esas lecturas no sirven. Con `vehiculo_id`, todas las lecturas de un vehículo quedan en una sola fila y se leen en el orden en que se enviaron. Esto importa porque queremos ver cómo cambian el kilometraje y la temperatura de cada vehículo con el tiempo, para asi saber si un vehículo está en riesgo. No usamos `tipo_vehiculo` porque solo tiene dos valores (moto y carro) y casi todo el tráfico quedaría en dos particiones. En cuanto a las particiones escogimos 3 porque con 1 no habría paralelismo y con 12 (una por vehículo) serían demasiadas para un solo servidor local. Con 3, cada una queda con unos 4 vehículos. En el ADR-001 hicimos una estimación, sin medir todavía, con 12 vehículos enviando una lectura cada 10 segundos serían 72 eventos por minuto, unos 24 por partición. En la Semana 2 la compararemos con lo que realmente envíe el producer.

## 4. Problema encontrado y cómo se resolvió
Al probar el `docker-compose.yml` con `docker compose up -d`, Docker mostró un error y era que el nombre de contenedor `rapidgo-kafka` ya estaba en uso por otro contenedor que ya existía en mi computador. Así que lo resolvimos cambiando el nombre del contenedor en el archivo a `rapidgo-v8-kafka`. Después `docker compose up -d` funcionó y `docker compose ps` mostró el contenedor de Kafka en estado Up. 

## 5. Riesgo principal
Los umbrales de las alertas (más de 5000 km desde el último mantenimiento y más de 110 °C) son suposiciones nuestras, todavía sin datos simulados. Si el producer genera valores que casi siempre los superan o que nunca los alcanzan, las alertas no servirían, los ajustaremos en la Semana 2 con las lecturas reales. También, si Kafka crea el topic solo, lo hace con 1 partición, así que lo crearemos a mano con 3 y lo verificaremos con `--describe`.

## 6. Plan de la Semana 2
- Crear el topic con 3 particiones y verificarlo.
- Producer de telemetría con key `vehiculo_id` (Issue #9).
- Spark Structured Streaming con 3 transformaciones y una agregación por ventana de 5 minutos (Issue #10).
- Guardar en Parquet con checkpoint e implementar las 2 alertas (Issue #11).
- Pruebas y `ci.yml` con GitHub Actions (Issue #12), y el ADR-002 con lo que observemos al ejecutar.
- Los responsables de cada Issue están asignados en el tablero.

## 7. Enlaces
- Issues: #1 a #8.
- Pull Requests: #16 (estructura y README), #21 (contrato), #17 (KPIs y alertas), #18 (arquitectura), #19 (ADR-001), #20 (docker-compose).