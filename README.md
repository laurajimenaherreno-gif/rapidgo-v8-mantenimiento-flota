# RapidGo - Variante 8: Mantenimiento de flota

**Proyecto final - Línea de Profundización I - Big Data**
Ingeniería de Sistemas y Computación - Facatativá

## Equipo
| Integrante | Usuario GitHub |
|---|---|
| Laura Jimena Herreño | @laurajimenaherreno-gif |
| Johan Alan Torres | @AlanT218 |
| Edwin Felipe Garavito | @felipegaravito888 |

## Variante
Variante 8: **Mantenimiento de flota**.

- Eventos: vehículo, kilometraje, temperatura, código de falla y uso.
- KPIs: km acumulados, fallas por tipo y vehículos en riesgo.
- Alertas: mantenimiento vencido y parámetro mecánico crítico.

## Problema de negocio
RapidGo es una plataforma de entregas. En la variante 8 se analiza el riesgo de falla
de los vehículos usados en el reparto. El pipeline recibe telemetría de cada vehículo
(kilometraje, temperatura, códigos de falla y uso), calcula cuántos kilómetros
acumula la flota, qué fallas ocurren y qué vehículos están en riesgo, y genera alertas
cuando hay un mantenimiento vencido o un parámetro mecánico crítico.

## Arquitectura
![Arquitectura del pipeline RapidGo V8](docs/arquitectura.png)

| Componente | Rol en la variante 8 |
|---|---|
| Producer (Python) | Simula los vehículos de reparto y envía cada lectura como evento JSON |
| Kafka | Recibe los eventos en el topic `rapidgo.flota.v8`, con key `vehiculo_id` y 3 particiones |
| Spark Structured Streaming | Lee de Kafka, parsea los eventos, aplica transformaciones y calcula KPIs por ventana de 5 minutos |
| Data Lake (Parquet) | Guarda los datos en las capas raw, processed y curated |
| Airflow | Orquesta el DAG `rapidgo_v8_control_flota`, con 5 tareas de verificación, calidad, resumen y reporte, con reintentos y XCom |
| Docker | Levanta Kafka, Spark y Airflow de forma local |
| GitHub Actions | Valida el código y el DAG automáticamente en cada Pull Request |

Transversal: Docker + Git + GitHub + Kanban + GitHub Actions + Releases.

## Orquestación con Airflow
El DAG `rapidgo_v8_control_flota` (carpeta `airflow/dags/`) revisa y resume los datos del Data Lake. Se ejecuta manualmente y tiene 5 tareas en este orden.

1. `verificar_fuentes` comprueba que existan datos en las capas raw, processed, curated/alertas y curated/kpi_vehiculo_ventana.
2. `calidad_datos` revisa la capa processed (nulos en campos clave, duplicados, temperaturas fuera de rango y kilometrajes inconsistentes) y frena el resto si los datos no pasan.
3. `resumen_kpis` y `resumen_alertas` resumen los KPIs por ventana y las alertas por motivo y vehículo.
4. `generar_reporte` guarda el reporte en `datalake/curated/reportes`.

Las tareas se pasan información con XCom y todas tienen 2 reintentos con 20 segundos de espera.

Para usarlo, con el pipeline ya ejecutado y el Data Lake con datos:

```bash
docker compose stop kafka
docker compose up -d airflow
```

Después se abre http://localhost:8080, se entra al DAG y se pulsa **Trigger**. Detener Kafka antes ahorra memoria, porque el DAG solo lee archivos del Data Lake.

Para probar el fallo controlado se crea en Airflow la variable `fallo_controlado` con valor `si`, y `calidad_datos` falla a propósito. Para recuperar, se cambia el valor a `no` y se limpia la tarea. La decisión está en el [ADR-003](docs/decisions/ADR-003-fallo-controlado-y-recuperacion.md).

## Pruebas y CI
Las pruebas cubren las reglas de negocio, el contrato del evento, el generador, la calidad de datos y la estructura del DAG. En local se ejecutan con:

```bash
py -m pytest -v
```

El workflow `CI RapidGo V8` corre en cada push a `main` y `develop`, en cada Pull Request hacia `main` y también de forma manual desde la pestaña Actions. Tiene dos jobs.

- `validar` revisa la sintaxis de Python, ejecuta las pruebas y valida el `docker-compose.yml`.
- `validar-dag` instala Airflow y ejecuta las pruebas del DAG.

## Documentación
- [Contrato del evento](docs/contrato-evento.md)
- [KPIs y reglas de alerta](docs/kpis-y-alertas.md)
- [ADR-001: key y particiones de Kafka](docs/decisions/ADR-001-key-y-particiones-kafka.md)
- [ADR-002: clave de partición vehiculo_id](docs/decisions/ADR-002-clave-particion-vehiculo-id.md)
- [ADR-003: fallo controlado y recuperación](docs/decisions/ADR-003-fallo-controlado-y-recuperacion.md)
- [Checkpoint Semana 1](docs/checkpoints/semana1.md)
- [Checkpoint Semana 2](docs/checkpoints/semana2.md)
- [Checkpoint Semana 3](docs/checkpoints/semana3.md)
- [Cómo ejecutar el pipeline en local](docs/ejecucion-local.md)
- [Decisiones de CI](docs/ci-decisiones.md)

## Estado del proyecto
- [x] Semana 1: diseño y organización (v0.1.0-foundation)
- [x] Semana 2: pipeline Kafka → Spark → Data Lake y CI (v0.2.0-streaming)
- [x] Semana 3: Airflow, calidad y resiliencia (v0.3.0-orchestration)
- [ ] Semana 4: integración y release v1.0.0

## Tablero Kanban
https://github.com/users/laurajimenaherreno-gif/projects/1
