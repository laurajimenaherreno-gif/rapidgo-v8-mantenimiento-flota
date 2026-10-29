# ADR-002 - Clave de partición vehiculo_id en el topic rapidgo.flota.v8

## Contexto / problema
Creamos el topic `rapidgo.flota.v8` con 3 particiones y replication factor 1. El producer manda las lecturas de los 12 vehículos y usa `vehiculo_id` como clave de cada mensaje. Cuando corrimos el producer vimos que los mensajes no se repartían parejo entre las particiones.

En la primera prueba (intervalo de 2 segundos durante 60 segundos) se generaron 372 eventos y quedaron 31 en la partición 0, 248 en la 1 y 93 en la 2. En la segunda (intervalo de 10 segundos durante 720 segundos) se generaron 876 y quedaron 73, 584 y 219. En las dos pruebas pasa lo mismo. La partición 1 se queda con el 66,7 % de los mensajes, la 2 con el 25 % y la 0 con apenas el 8,3 %. Si el reparto fuera parejo tendrían que ser 124 por partición en la primera prueba y 292 en la segunda.

Con el script de verificación vimos por qué. Cada vehículo cae siempre en la misma partición, pero los 12 vehículos no se reparten igual. En la partición 0 solo está VEH-007, en la 2 hay tres vehículos y en la 1 hay ocho. Los errores de entrega fueron 0 en las dos pruebas, o sea que no se perdió nada y el problema es solo el reparto. Tuvimos que decidir si dejábamos la clave como estaba o si cambiábamos la forma de repartir.

## Opciones consideradas
1. Dejar `vehiculo_id` como clave con 3 particiones. Todos los mensajes de un vehículo van a la misma partición y se leen en el orden en que llegaron. Es lo que ya tenemos y no hay que cambiar nada. Lo malo es el reparto desparejo que vimos.
2. Mandar los mensajes sin clave para que Kafka los reparta parejo entre las 3 particiones. El reparto mejoraría, pero las lecturas de un mismo vehículo quedarían en varias particiones y podrían llegar desordenadas. Eso complica calcular los km acumulados y las alertas de mantenimiento por vehículo.
3. Aumentar las particiones (por ejemplo a 12) y seguir usando `vehiculo_id` como clave. Podría repartir mejor, pero no lo probamos y con solo 12 claves no hay garantía de que quede parejo. Además es más carga para un Kafka de un solo broker que corre en nuestros equipos.

## Decisión tomada
Elegimos la opción 1. Dejamos `vehiculo_id` como clave y el topic con 3 particiones. Preferimos tener las lecturas de cada vehículo en orden, porque las alertas de mantenimiento y los km acumulados dependen de eso, aunque una partición reciba más mensajes que las otras. Con 12 vehículos y una sola máquina el desbalance es aceptable por ahora, y en las pruebas no hubo errores de entrega.

## Evidencia utilizada
Topic creado (`kafka-topics.sh --describe`)

```
Topic: rapidgo.flota.v8  PartitionCount: 3  ReplicationFactor: 1
```

Producer, corrida 1 (`py producer/producer_flota.py --intervalo 2 --duracion 60`)

```
Eventos generados: 372
Entregados por particion: {0: 31, 1: 248, 2: 93}
Errores de entrega: 0
```

Producer, corrida 2 (`py producer/producer_flota.py --intervalo 10 --duracion 720`)

```
Eventos generados: 876
Entregados por particion: {0: 73, 1: 584, 2: 219}
Errores de entrega: 0
```

Resumen del reparto

| Partición | Vehículos | Corrida 1 | % | Corrida 2 | % |
|---|---|---|---|---|---|
| 0 | 1 | 31 | 8,3 % | 73 | 8,3 % |
| 1 | 8 | 248 | 66,7 % | 584 | 66,7 % |
| 2 | 3 | 93 | 25,0 % | 219 | 25,0 % |
| Total | 12 | 372 | 100 % | 876 | 100 % |

Primer lote de Spark (columna value omitida)

```
--- lote 0: 372 mensajes leidos de Kafka ---
key      partition  offset
VEH-001  1          0
VEH-003  1          1
VEH-004  1          2
VEH-005  1          3
VEH-006  1          4
```

Verificación del data lake (`docker compose run --rm verificar`)

```
=== raw: 96 mensajes ===
partition  mensajes  offset_min  offset_max
0          8         31          38
1          64        248         311
2          24        93          116

Vehiculos (key) por particion:
0  [VEH-007]
1  [VEH-001, VEH-003, VEH-004, VEH-005, VEH-006, VEH-010, VEH-011, VEH-012]
2  [VEH-002, VEH-008, VEH-009]
```

Capturas

- <img width="921" height="151" alt="image" src="https://github.com/user-attachments/assets/8e0f2823-7795-44a2-84fd-f709ee57d2e4" />

- <img width="921" height="271" alt="image" src="https://github.com/user-attachments/assets/d5b04bd1-8212-44ab-ac0e-b202e81ece43" />

- <img width="921" height="181" alt="image" src="https://github.com/user-attachments/assets/99ccfbe1-aa75-4a25-94a5-f6b22549f2d6" />

- <img width="921" height="398" alt="image" src="https://github.com/user-attachments/assets/0e53470b-be6f-411f-93d9-202af6ee9ca2" />

- <img width="921" height="374" alt="image" src="https://github.com/user-attachments/assets/22d2f224-3895-4977-96c2-4feba86c23e7" />

Pull Request de este ADR: https://github.com/laurajimenaherreno-gif/rapidgo-v8-mantenimiento-flota/pull/28

## Consecuencias / trade-offs
Lo que ganamos es que cada vehículo queda siempre en una sola partición y sus lecturas se mantienen en orden. Se ve en la tabla de verificación, donde ningún vehículo aparece en dos particiones. Tampoco hubo que cambiar el código del producer.

El riesgo que queda es que la partición 1 recibe dos tercios del tráfico y la 0 casi no se usa. Como Spark normalmente lee cada partición de Kafka como una tarea aparte, la partición 1 haría casi todo el trabajo y podría volverse un cuello de botella si suben los vehículos o la frecuencia de envío. Con solo 12 claves el reparto depende del hash de cada una y no lo controlamos.

También vimos en Spark el aviso de que el lote se estaba atrasando. Tardó 18425 y 26697 milisegundos con un intervalo de 10000, después de leer los 372 mensajes del primer lote. No sabemos si tiene relación con el reparto de particiones, así que lo dejamos anotado para revisarlo más adelante.
