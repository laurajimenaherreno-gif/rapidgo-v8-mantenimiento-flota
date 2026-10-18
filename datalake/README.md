# Data Lake RapidGo - Variante 8

Estructura por capas:

- `raw/`: eventos de telemetría de flota tal como llegan desde Kafka, sin modificar.
- `processed/`: eventos limpios, tipados y transformados (km recorridos, tipo de falla, severidad).
- `curated/`: agregaciones y KPIs listos para consumo (km acumulados, fallas por tipo, vehículos en riesgo).

Los datos generados NO se suben a GitHub (ver .gitignore).