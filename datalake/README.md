# Data Lake RapidGo - Variante 8

Estructura por capas (Parquet):

- `raw/`: mensajes de Kafka tal como llegan (key, value en JSON, topic, particion y offset). Esta particionado por `partition=0|1|2`.
- `processed/`: eventos validos, parseados y transformados (km desde el mantenimiento, tipo y severidad de falla, banderas de alerta y de riesgo).
- `curated/alertas/`: solo los eventos que activan una alerta (mantenimiento vencido o parametro critico), con `motivo_alerta`.
- `curated/kpi_vehiculo_ventana/`: KPIs por vehiculo y ventana de tiempo (km acumulados, fallas por tipo, vehiculos en riesgo).
- `_checkpoints/`: estado interno de Spark (offsets leidos y estado de las ventanas). No se debe borrar mientras el job corre.

Los datos generados NO se suben a GitHub (ver .gitignore).
