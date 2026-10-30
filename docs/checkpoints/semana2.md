# Checkpoint Semana 2

## 1. Qué quedó funcionando esta semana
Ya tenemos el recorrido Kafka → Spark → Data Lake funcionando en local. Creamos el topic `rapidgo.flota.v8` con 3 particiones, el producer envía las lecturas de los 12 vehículos con `vehiculo_id` como key, y Spark las lee, las transforma, calcula las 2 alertas, agrupa los KPIs por ventana de 5 minutos y guarda todo en Parquet con checkpoint. También hicimos las pruebas de las reglas de negocio y el workflow de GitHub Actions, que corre en cada Pull Request.

## 2. Valores reales de la ejecución
- Segunda corrida del producer (intervalo de 10 s durante 720 s): 876 eventos, unos 73 por minuto. En la Semana 1 estimamos 72 por minuto. Errores de entrega: 0.
- Entregados por partición: {0: 73, 1: 584, 2: 219}. Esperábamos unos 24 por minuto en cada una, pero fueron cerca de 6, 49 y 18.
- Al reiniciar Spark desde cero, `verificar` mostró `raw` con 1248 mensajes (104, 832 y 312 por partición, offsets desde 0) y `processed` con los mismos 1248, sin descartar ninguno.
- Hubo 251 eventos con alerta: mantenimiento vencido sobre todo en `VEH-003` (104) y `VEH-008` (102), y parámetro crítico repartido entre varios vehículos (por ejemplo `VEH-004` con 8).
- `kpi_vehiculo_ventana` quedó con 48 filas. Por ejemplo, `VEH-001` en la ventana de 05:10 a 05:15: 31 lecturas, 7,73 km recorridos, temperatura máxima de 123,2 °C y 1 falla de motor.

## 3. Decisión técnica y por qué
Dejamos `vehiculo_id` como key con 3 particiones, aunque el reparto quedó desparejo: la partición 1 tiene 8 vehículos y el 66,7 % de los mensajes, la 2 tiene 3 y la 0 solo a `VEH-007`. Preferimos que las lecturas de cada vehículo lleguen en orden, porque los km acumulados y las alertas dependen de eso. Los datos y las opciones descartadas están en el ADR-002 (PR #28).

## 4. Problemas encontrados
- **Reparto desparejo:** en las dos corridas la partición 1 recibió el 66,7 % de los mensajes. Cada vehículo siempre cae en la misma partición, pero los 12 no se reparten parejo. Lo analizamos y decidimos en el PR #28.
- **Checkpoint viejo y KPI vacío:** la primera vez que verificamos, `raw` empezaba en los offsets 31, 248 y 93 y el KPI estaba vacío, porque Spark había retomado desde el checkpoint de la prueba en consola y solo llevaba 96 mensajes. Lo resolvimos borrando datalake/_checkpoints y reiniciando Spark.
- **Spark se atrasa:** avisó que un lote tardó 18425 y 26697 ms con un intervalo de 10000 ms. No sabemos la causa y lo revisaremos en la Semana 3.

## 5. Enlaces
- Issues: #9, #10, #11, #12, #23 y #24
- Pull Requests: #25 (reglas, pruebas y CI), #26 (producer), #27 (Spark) y #28 (ADR-002).
- GitHub Actions: pestaña Actions, workflow "CI RapidGo V8".