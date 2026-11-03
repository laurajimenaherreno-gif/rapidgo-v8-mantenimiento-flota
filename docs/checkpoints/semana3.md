# Checkpoint Semana 3

## 1. Qué quedó funcionando esta semana
Ya tenemos el recorrido Kafka → Spark → Data Lake funcionando en local. Creamos el topic `rapidgo.flota.v8` con 3 particiones, el producer envía las lecturas de los 12 vehículos con `vehiculo_id` como key, y Spark las lee, las transforma, calcula las 2 alertas, agrupa los KPIs por ventana de 5 minutos y guarda todo en Parquet con checkpoint. También hicimos las pruebas de las reglas de negocio y el workflow de GitHub Actions, que corre en cada Pull Request.

## 2. Valores reales de la ejecución
El DAG revisó 1248 filas de la capa processed y la calidad quedó aprobada, con 0 filas con nulos, 0 duplicados, 0 temperaturas fuera de rango y 0 km inconsistentes. Con el fallo controlado activo, la ejecución tardó 45 segundos en terminar como Fallido, y al recuperarla calidad_datos terminó bien en el intento 4, en 2.037 segundos.

## 3. Decisión técnica y por qué
Provocamos el fallo con una variable de Airflow llamada fallo_controlado, en vez de dañar datos o cambiar el código, porque se activa y se quita desde la interfaz sin tocar nada más. La decisión completa está en el ADR-003 (docs/decisions/ADR-003-fallo-controlado-y-recuperacion.md).

## 4. Problemas encontrados
Con el fallo activado, los 2 reintentos no sirven porque el error se repite igual en cada intento, y la ejecución tardó 45 segundos en marcarse como fallida. Quedó documentado en el ADR-003, en el PR (https://github.com/laurajimenaherreno-gif/rapidgo-v8-mantenimiento-flota/pull/33).

## 5. Enlaces
- Issues: #13 (DAG de Airflow), #14 (fallo controlado, recuperación y CI ampliado) y #34 (checkpoint)
- Pull Requests: #30 (DAG de control de la flota), #31 (job validar-dag en CI) y #33 (ADR-003).
- GitHub Actions: pestaña Actions, workflow "CI RapidGo V8" (job validar-dag).
