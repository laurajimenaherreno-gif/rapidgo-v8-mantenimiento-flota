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
| Airflow | Orquesta las tareas de calidad, agregación y reporte |
| Docker | Levanta todos los servicios de forma local |
| GitHub Actions | Valida el código automáticamente en cada Pull Request |

Transversal: Docker + Git + GitHub + Kanban + GitHub Actions + Releases.

## Documentación
- [Contrato del evento](docs/contrato-evento.md)
- [KPIs y reglas de alerta](docs/kpis-y-alertas.md)
- [ADR-001: key y particiones de Kafka](docs/decisions/ADR-001-key-y-particiones-kafka.md)
- [Checkpoint Semana 1](docs/checkpoints/semana1.md)

## Estado del proyecto
- [x] Semana 1: diseño y organización (v0.1.0-foundation)
- [ ] Semana 2: pipeline Kafka → Spark → Data Lake y CI
- [ ] Semana 3: Airflow, calidad y resiliencia
- [ ] Semana 4: integración y release v1.0.0

## Tablero Kanban
https://github.com/users/laurajimenaherreno-gif/projects/1
