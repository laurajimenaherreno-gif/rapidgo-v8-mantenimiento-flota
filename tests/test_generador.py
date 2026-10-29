"""Pruebas del generador de eventos del producer (sin Kafka)."""
import random

from generador import crear_flota, siguiente_evento
from reglas import mantenimiento_vencido, validar_evento


def generar(semilla, ciclos):
    rng = random.Random(semilla)
    flota = crear_flota(12, rng)
    eventos = []
    for _ in range(ciclos):
        for vehiculo in flota:
            eventos.append(siguiente_evento(vehiculo, rng))
    return eventos


def test_la_flota_tiene_12_vehiculos_distintos():
    flota = crear_flota(12, random.Random(1))
    ids = [v["vehiculo_id"] for v in flota]
    assert len(ids) == 12
    assert len(set(ids)) == 12
    assert ids[0] == "VEH-001" and ids[-1] == "VEH-012"


def test_todos_los_eventos_generados_cumplen_el_contrato():
    for evento in generar(semilla=7, ciclos=50):
        assert validar_evento(evento) == []


def test_con_la_misma_semilla_se_repite_la_misma_simulacion():
    a = generar(semilla=42, ciclos=5)
    b = generar(semilla=42, ciclos=5)
    # el timestamp depende del reloj; todo lo demas debe coincidir
    for x, y in zip(a, b):
        x.pop("timestamp")
        y.pop("timestamp")
    assert a == b


def test_el_kilometraje_de_cada_vehiculo_nunca_baja():
    # Esto es lo que protege la key = vehiculo_id: si las lecturas de un vehiculo
    # llegan en orden, el kilometraje solo crece.
    ultimo = {}
    for evento in generar(semilla=3, ciclos=100):
        vid = evento["vehiculo_id"]
        assert evento["kilometraje"] >= ultimo.get(vid, 0)
        ultimo[vid] = evento["kilometraje"]


def test_la_simulacion_incluye_vehiculos_con_mantenimiento_vencido():
    flota = crear_flota(12, random.Random(5))
    vencidos = [v for v in flota
                if mantenimiento_vencido(v["kilometraje"], v["km_ultimo_mantenimiento"])]
    assert len(vencidos) >= 2
