"""Pruebas del contrato del evento (docs/contrato-evento.md)."""
import json
from pathlib import Path

import pytest

from reglas import CAMPOS_EVENTO, validar_evento

RAIZ = Path(__file__).resolve().parent.parent
EJEMPLO = RAIZ / "samples" / "evento_flota.json"


def cargar_ejemplo():
    return json.loads(EJEMPLO.read_text(encoding="utf-8"))


def test_el_ejemplo_del_repositorio_cumple_el_contrato():
    assert validar_evento(cargar_ejemplo()) == []


def test_el_ejemplo_tiene_exactamente_los_campos_del_contrato():
    assert set(cargar_ejemplo().keys()) == set(CAMPOS_EVENTO)


def test_codigo_falla_nulo_es_valido():
    evento = cargar_ejemplo()
    evento["codigo_falla"] = None
    assert validar_evento(evento) == []


def test_caso_limite_km_ultimo_igual_a_kilometraje_es_valido():
    evento = cargar_ejemplo()
    evento["km_ultimo_mantenimiento"] = evento["kilometraje"]
    assert validar_evento(evento) == []


@pytest.mark.parametrize("campo", [
    "event_id", "vehiculo_id", "tipo_vehiculo", "timestamp", "kilometraje",
    "km_ultimo_mantenimiento", "temperatura_motor_c", "horas_uso",
])
def test_falta_un_campo_obligatorio_es_invalido(campo):
    evento = cargar_ejemplo()
    del evento[campo]
    assert validar_evento(evento) != []


@pytest.mark.parametrize("campo, valor", [
    ("vehiculo_id", "VEH-013"),
    ("vehiculo_id", "CAMION-1"),
    ("tipo_vehiculo", "bicicleta"),
    ("timestamp", "19/10/2026 14:20"),
    ("kilometraje", -5),
    ("km_ultimo_mantenimiento", 99999.0),   # mayor que kilometraje
    ("temperatura_motor_c", 250),
    ("horas_uso", 30),
    ("codigo_falla", "P9999"),              # no esta en el catalogo
    ("event_id", "no-es-un-uuid"),
])
def test_valores_fuera_del_contrato_son_invalidos(campo, valor):
    evento = cargar_ejemplo()
    evento[campo] = valor
    assert validar_evento(evento) != []
