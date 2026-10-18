# ADR-001 - Key y particiones del topic de telemetría de flota

## Contexto / problema
La variante 8 analiza el riesgo de falla de los vehículos de reparto. Para detectar mantenimiento vencido y temperaturas críticas sostenidas, las lecturas de un mismo vehículo deben conservar su orden (el kilometraje solo crece y la temperatura se evalúa en secuencia) y procesarse juntas. Hay que decidir cómo se nombra el topic, qué campo se usa como key y cuántas particiones tendrá.

## Opciones consideradas
1. Key = `tipo_vehiculo`, 3 particiones: solo hay 2 valores (moto, carro), así que casi todo el tráfico caería en 2 particiones (desbalance) y se mezclarían lecturas de vehículos distintos.
2. Key = `vehiculo_id`, 3 particiones: todas las lecturas de un vehículo van a la misma partición y mantienen el orden; el tráfico se reparte entre los 12 vehículos.
3. Sin key: Kafka reparte los mensajes sin criterio, sin garantía de orden por vehículo.

## Decisión tomada
- Topic: `rapidgo.flota.v8`
- Key: `vehiculo_id`
- Particiones: 3
- Factor de replicación: 1 (entorno local con un solo broker)

## Evidencia utilizada
- Flota simulada: 12 vehículos (`VEH-001` a `VEH-012`).
- Frecuencia: cada vehículo envía 1 lectura cada 10 segundos, es decir 6 lecturas por minuto por vehículo y 72 eventos por minuto en total (aprox. 1,2 eventos por segundo).
- Con 3 particiones, el promedio es de 4 vehículos por partición (24 eventos por minuto por partición). Es una carga baja, que un solo broker local maneja sin problema.
- Tres particiones permiten probar el procesamiento en paralelo con Spark sin complicar el entorno local; más particiones no aportan nada con este volumen.
- Kafka decide la partición con un hash de la key, así que 12 vehículos no se reparten necesariamente en 4-4-4. Esto se verificará con los offsets por partición en la Semana 2.
- Se eligió una lectura cada 10 segundos por vehículo porque es suficiente para ver cambios de temperatura dentro de una ventana de 5 minutos (30 lecturas por vehículo) sin generar un volumen difícil de revisar en las pruebas locales.

## Consecuencias / trade-offs
- Ventaja: se garantiza el orden por vehículo y las agregaciones por vehículo (km recorridos, temperatura en el tiempo) son más simples.
- Riesgo: el hash puede dejar más vehículos en una partición que en otra, y un vehículo que emita muchos más eventos sobrecargaría su partición.
- Con replicación 1, si el broker se cae no hay copia de los datos. Es aceptable en un entorno local de pruebas.
- Cambiar el número de particiones más adelante modifica a qué partición va cada key, y se perdería el orden con los datos ya guardados; por eso conviene decidirlo bien ahora.
- Esta decisión se revisará en la Semana 2 con los datos reales de ejecución.