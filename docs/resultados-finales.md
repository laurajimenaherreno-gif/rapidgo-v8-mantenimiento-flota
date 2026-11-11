# Resultados de la integración final

## Prueba desde una copia limpia
La prueba la hicimos el 10 de noviembre de 2026. Clonamos el repositorio en una carpeta nueva llamada rapidgo-prueba-limpia y seguimos los pasos de docs/ejecucion-local.md. Levantamos Kafka, creamos el topic, corrimos Spark, el producer y la verificación, y al final ejecutamos el DAG en Airflow.

Estas cosas fallaron o faltaban en el camino.

- El topic no se crea solo, hay que crearlo a mano con 3 particiones antes de correr Spark y el producer.
- El archivo docs/ejecucion-local.md estaba desactualizado. Decía Semana 2, no hablaba de Airflow y tenía una línea mal pegada entre la verificación y el apagado.
- En Airflow la tarea generar_reporte falló con PermissionError al crear la carpeta datalake/curated/reportes, porque esa carpeta la creó el contenedor de Spark y Airflow no tenía permiso para escribir ahí. Lo resolví dándole permisos de escritura a datalake/curated desde el contenedor y limpiando la tarea para que corriera de nuevo.
- Spark mostró muchos avisos de que el lote iba atrasado. Cada lote tardó entre 10,2 y 24,0 segundos con un intervalo de 10. No hubo errores y los datos se procesaron completos.
- Al detener Spark con Ctrl + C salió un traceback con KeyboardInterrupt, que es normal porque lo interrumpí yo.

## Kafka y particiones
Ejecutamos el producer con `py producer/producer_flota.py --intervalo 10 --duracion 600` y esto fue lo que imprimió.

```
Producer iniciado. Topic: rapidgo.flota.v8 | vehiculos: 12 | intervalo: 10.0s
Eventos generados: 732
Entregados por particion: {0: 61, 1: 488, 2: 183}
Errores de entrega: 0
```

| Partición | Vehículos | Mensajes | % |
|---|---|---|---|
| 0 | 1 | 61 | 8,3 % |
| 1 | 8 | 488 | 66,7 % |
| 2 | 3 | 183 | 25,0 % |
| Total | 12 | 732 | 100 % |

El reparto sigue siendo desparejo y es igual al del ADR-002, porque cada vehículo siempre cae en la misma partición. No hubo errores de entrega.

## Spark y Data Lake
Spark arrancó con ventana de 5 minutos, watermark de 1 minuto y salida en Parquet. La verificación con `docker compose run --rm verificar` mostró estos conteos por capa.

- raw tuvo 732 mensajes, con offsets del 0 al 60 en la partición 0, del 0 al 487 en la 1 y del 0 al 182 en la 2.
- processed tuvo 732 eventos válidos, o sea los mismos que llegaron a raw, así que no se perdió ninguno.
- alertas tuvo 144 eventos con alerta. Los mayores fueron MANTENIMIENTO_VENCIDO en VEH-003 y VEH-008 con 60 cada uno, y PARAMETRO_CRITICO sobre todo en VEH-007 con 7 y VEH-004 con 4.
- kpi_vehiculo_ventana tuvo 24 filas, una por vehículo y ventana cerrada. La primera ventana, de 03:00 a 03:05, tuvo 22 lecturas por cada vehículo.

Los vehículos por partición fueron VEH-007 en la 0, ocho vehículos en la 1 y VEH-002, VEH-008 y VEH-009 en la 2.

## Airflow
En la primera ejecución del DAG rapidgo_v8_control_flota, 4 de las 5 tareas quedaron en verde y generar_reporte falló por el problema de permisos explicado arriba. Después de corregirlo y limpiar la tarea, el DAG terminó exitosamente en 00:00:11.287. 

## CI
Último run de main con los jobs validar y validar-dag en verde. Cuando se integre el CI final se suma el job quality-gate.

## Capturas
- docs/evidencias/final/final_01_producer.png
- docs/evidencias/final/final_02_spark.png
- docs/evidencias/final/final_03_verificar.png
- docs/evidencias/final/final_04_airflow_dag.png
- docs/evidencias/final/final_05_reporte.png