"""Pruebas de las reglas de negocio de la variante 8 (sin Kafka ni Spark)."""
import pytest

from reglas import (
    clasificar_estado,
    falla_critica,
    km_desde_mantenimiento,
    mantenimiento_vencido,
    parametro_critico,
    vehiculo_en_riesgo,
)


# --- Alerta 1: mantenimiento vencido ----------------------------------------
def test_mantenimiento_normal_no_esta_vencido():
    # 18450.7 - 14200.0 = 4250.7 km: todavia dentro del intervalo de 5000 km
    assert mantenimiento_vencido(18450.7, 14200.0) is False


def test_mantenimiento_en_el_limite_exacto_no_esta_vencido():
    # exactamente 5000 km: la regla es "mayor que", asi que aun no vence
    assert mantenimiento_vencido(15000.0, 10000.0) is False


def test_mantenimiento_vencido_al_pasar_el_limite():
    assert mantenimiento_vencido(15000.1, 10000.0) is True


def test_km_ultimo_mayor_que_kilometraje_es_invalido():
    with pytest.raises(ValueError):
        km_desde_mantenimiento(1000.0, 2000.0)


# --- Alerta 2: parametro mecanico critico -----------------------------------
def test_temperatura_normal_sin_falla_no_es_critica():
    assert parametro_critico(95.0, None) is False


def test_temperatura_en_el_limite_exacto_no_es_critica():
    # 110.0 no supera el limite (la regla es "mayor que")
    assert parametro_critico(110.0, None) is False


def test_temperatura_sobre_el_limite_es_critica():
    assert parametro_critico(110.1, None) is True


@pytest.mark.parametrize("codigo", ["P0217", "P0524"])
def test_codigo_de_severidad_critica_activa_la_alerta(codigo):
    assert parametro_critico(90.0, codigo) is True


def test_codigo_de_severidad_media_no_activa_la_alerta():
    assert falla_critica("P0300") is False
    assert parametro_critico(90.0, "P0300") is False


def test_codigo_desconocido_no_se_considera_critico():
    assert falla_critica("XXXX") is False


# --- KPI: vehiculos en riesgo -----------------------------------------------
def test_vehiculo_sano_no_esta_en_riesgo():
    assert vehiculo_en_riesgo(12000.0, 10000.0, 90.0, None) is False


def test_vehiculo_en_riesgo_por_cercania_al_mantenimiento():
    # 4500 km = 90 % del intervalo
    assert vehiculo_en_riesgo(14500.0, 10000.0, 90.0, None) is True


def test_vehiculo_en_riesgo_por_temperatura_alta():
    assert vehiculo_en_riesgo(12000.0, 10000.0, 100.0, None) is True


# --- Estado resumido ---------------------------------------------------------
@pytest.mark.parametrize("km, ult, temp, codigo, esperado", [
    (12000.0, 10000.0, 90.0, None, "NORMAL"),
    (14500.0, 10000.0, 90.0, None, "EN_RIESGO"),
    (15500.0, 10000.0, 90.0, None, "VENCIDO"),
    (12000.0, 10000.0, 112.4, "P0217", "CRITICO"),
    (15500.0, 10000.0, 115.0, None, "CRITICO"),  # critico tiene prioridad sobre vencido
])
def test_clasificar_estado(km, ult, temp, codigo, esperado):
    assert clasificar_estado(km, ult, temp, codigo) == esperado
