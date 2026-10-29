# Decisiones de CI - Variante 8 (Mantenimiento de flota)

El workflow `.github/workflows/ci.yml` se ejecuta en cada `push` a `main` o `develop`, en cada Pull Request hacia `main` y de forma manual (`workflow_dispatch`). Cada verificacion existe para evitar un error concreto del proyecto.

## Que verifica y por que

| Verificacion | Que comprueba | Error que evita |
|---|---|---|
| `python -m compileall producer spark tests` | Que todos los `.py` de `producer/`, `spark/` y `tests/` tengan sintaxis valida | Subir un archivo con un error de sintaxis que rompa el producer o el job de Spark |
| `pytest` - `tests/test_reglas.py` | Las reglas de negocio: mantenimiento vencido (> 5000 km), parametro critico (> 110 C o falla CRITICA) y vehiculo en riesgo | Cambiar un umbral o una condicion sin darse cuenta y alterar las alertas |
| `pytest` - `tests/test_contrato.py` | Que `samples/evento_flota.json` cumpla el contrato y que los eventos invalidos se rechacen | Que el ejemplo, el contrato y el codigo se desincronicen |
| `pytest` - `tests/test_generador.py` | Que el producer genere eventos validos y que el kilometraje de un vehiculo nunca baje | Enviar eventos que Spark descartaria, o desordenar las lecturas de un vehiculo |
| `docker compose config --quiet` | Que `docker-compose.yml` sea valido | Subir un compose roto que impida levantar el entorno |

## Las cinco decisiones que pide la guia de Actions

1. **Regla de negocio probada sin Kafka:** las reglas de `spark/reglas.py` (`mantenimiento_vencido`, `parametro_critico`, `vehiculo_en_riesgo` y `clasificar_estado`).
2. **Casos de prueba:** para cada alerta hay un caso normal, un caso limite (exactamente 5000 km o exactamente 110 C no activan la alerta) y un caso invalido o de alerta.
3. **Carpetas validadas con `compileall`:** `producer`, `spark` y `tests`.
4. **Archivos comprobados automaticamente:** `samples/evento_flota.json` (contra el contrato) y `docker-compose.yml`.
5. **Condicion que impide el merge:** el job `validar` debe terminar en verde y el Pull Request debe tener la aprobacion de otro integrante.

## Que NO se ejecuta en CI

No se levanta Kafka ni Spark en cada push: seria lento y fragil. Ese flujo completo se valida de forma local con Docker y su evidencia queda en los checkpoints.
