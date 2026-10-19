# Contrato del evento: telemetría de vehículo (Variante 8 - Mantenimiento de flota)

Cada evento es una lectura de telemetría de un vehículo de reparto (moto o carro).

| Campo | Tipo | Obligatorio | Descripción | Valores / formato |
|---|---|---|---|---|
| event_id | string (UUID) | Sí | Identificador único del evento | UUID v4 |
| vehiculo_id | string | Sí | Identificador del vehículo | `VEH-001` a `VEH-012` |
| tipo_vehiculo | string | Sí | Tipo de vehículo | moto, carro |
| timestamp | string (ISO 8601 UTC) | Sí | Momento de la lectura | `2026-10-19T14:20:05Z` |
| kilometraje | number | Sí | Kilometraje total (odómetro) en km | >= 0 |
| km_ultimo_mantenimiento | number | Sí | Kilometraje al que se hizo el último mantenimiento | >= 0 y <= kilometraje |
| temperatura_motor_c | number | Sí | Temperatura del motor en °C | 0 a 200 |
| codigo_falla | string | No | Código de falla reportado; `null` si no hay falla | Ver catálogo |
| horas_uso | number | Sí | Horas de uso acumuladas del vehículo en el día | 0 a 24 |

## Catálogo de códigos de falla (simplificado para el proyecto)
| Código | Tipo de falla | Severidad | Descripción |
|---|---|---|---|
| P0217 | MOTOR | CRITICA | Sobrecalentamiento del motor |
| P0524 | LUBRICACION | CRITICA | Presión de aceite muy baja |
| P0300 | MOTOR | MEDIA | Fallo de encendido (misfire) |
| P0562 | ELECTRICO | MEDIA | Voltaje bajo del sistema |
| P0171 | COMBUSTIBLE | BAJA | Mezcla de combustible pobre |

## Reglas de validación
- Todos los campos son obligatorios excepto `codigo_falla`, que puede ser `null` (sin falla).
- `km_ultimo_mantenimiento` no puede ser mayor que `kilometraje`.
- Si `codigo_falla` no es nulo, debe existir en el catálogo.
- `tipo_vehiculo` debe ser `moto` o `carro`.
