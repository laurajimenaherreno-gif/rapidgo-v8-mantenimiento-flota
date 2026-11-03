"""Pruebas de las funciones de calidad, resumen y reporte del DAG (sin Airflow)."""
import os
from datetime import datetime

import pytest

from calidad_flota import (
    construir_reporte,
    evaluar_calidad,
    guardar_reporte,
    resumir_alertas,
    resumir_kpis,
    verificar_fuentes,
)


def fila(n=1, **cambios):
    """Una fila valida de processed; los cambios permiten volverla invalida."""
    base = {
        "event_id": "evento-%d" % n,
        "vehiculo_id": "VEH-001",
        "ts_evento": datetime(2026, 10, 29, 5, 20, 27),
        "kilometraje": 12000.0,
        "km_ultimo_mantenimiento": 10000.0,
        "temperatura_motor_c": 92.0,
    }
    base.update(cambios)
    return base


# --- verificar_fuentes -------------------------------------------------------
def crear_parquet_falso(base, carpeta):
    destino = os.path.join(base, carpeta)
    os.makedirs(destino, exist_ok=True)
    with open(os.path.join(destino, "part-0000.parquet"), "wb") as archivo:
        archivo.write(b"x")


def test_fuentes_completas_devuelven_el_conteo_por_capa(tmp_path):
    for carpeta in ("raw", "processed", "curated/alertas", "curated/kpi_vehiculo_ventana"):
        crear_parquet_falso(str(tmp_path), carpeta)
    resultado = verificar_fuentes(str(tmp_path))
    assert resultado["processed"] == 1
    assert len(resultado) == 4


def test_fuente_faltante_lanza_error_con_el_nombre_de_la_capa(tmp_path):
    crear_parquet_falso(str(tmp_path), "raw")
    with pytest.raises(FileNotFoundError) as error:
        verificar_fuentes(str(tmp_path))
    assert "processed" in str(error.value)


def test_carpeta_sin_archivos_parquet_cuenta_como_faltante(tmp_path):
    os.makedirs(os.path.join(str(tmp_path), "raw"))
    with pytest.raises(FileNotFoundError):
        verificar_fuentes(str(tmp_path), fuentes=("raw",))


# --- evaluar_calidad ---------------------------------------------------------
def test_datos_sanos_son_aprobados():
    resultado = evaluar_calidad([fila(1), fila(2), fila(3)])
    assert resultado["aprobado"] is True
    assert resultado["total_filas"] == 3
    assert resultado["problemas"] == []


def test_temperatura_en_el_limite_exacto_es_valida():
    assert evaluar_calidad([fila(1, temperatura_motor_c=200)])["aprobado"] is True
    assert evaluar_calidad([fila(1, temperatura_motor_c=0)])["aprobado"] is True


def test_temperatura_fuera_de_rango_es_rechazada():
    resultado = evaluar_calidad([fila(1, temperatura_motor_c=250.0)])
    assert resultado["aprobado"] is False
    assert resultado["temperatura_fuera_de_rango"] == 1


def test_campo_clave_vacio_es_rechazado():
    resultado = evaluar_calidad([fila(1, vehiculo_id=None)])
    assert resultado["aprobado"] is False
    assert resultado["filas_con_nulos"] == 1


def test_km_ultimo_mayor_que_kilometraje_es_rechazado():
    resultado = evaluar_calidad([fila(1, km_ultimo_mantenimiento=99999.0)])
    assert resultado["km_inconsistentes"] == 1
    assert resultado["aprobado"] is False


def test_un_duplicado_en_cien_filas_esta_en_el_limite_y_se_acepta():
    filas = [fila(n) for n in range(99)] + [fila(0)]   # 1 repetido de 100 = 1 %
    resultado = evaluar_calidad(filas)
    assert resultado["duplicados"] == 1
    assert resultado["aprobado"] is True


def test_dos_duplicados_en_cien_filas_superan_el_limite():
    filas = [fila(n) for n in range(98)] + [fila(0), fila(1)]   # 2 %
    assert evaluar_calidad(filas)["aprobado"] is False


def test_sin_filas_es_rechazado():
    resultado = evaluar_calidad([])
    assert resultado["aprobado"] is False
    assert "no tiene filas" in resultado["problemas"][0]


# --- resumir_kpis ------------------------------------------------------------
def kpi(vehiculo, inicio, km, recorridos, riesgo, **fallas):
    datos = {"vehiculo_id": vehiculo, "ventana_inicio": inicio, "km_acumulados": km,
             "km_recorridos_ventana": recorridos, "en_riesgo": riesgo,
             "fallas_motor": 0, "fallas_lubricacion": 0,
             "fallas_electrico": 0, "fallas_combustible": 0}
    datos.update(fallas)
    return datos


def test_resumen_de_kpis_usa_el_km_mayor_y_la_ultima_ventana():
    filas = [
        kpi("VEH-001", datetime(2026, 10, 29, 5, 10), 1000.0, 5.0, 1, fallas_motor=1),
        kpi("VEH-001", datetime(2026, 10, 29, 5, 15), 1010.0, 10.0, 0),
        kpi("VEH-002", datetime(2026, 10, 29, 5, 10), 2000.0, 4.0, 1, fallas_electrico=2),
    ]
    resumen = resumir_kpis(filas)
    assert resumen["ventanas"] == 2
    assert resumen["vehiculos"] == 2
    assert resumen["km_flota_acumulados"] == 3010.0      # 1010 + 2000
    assert resumen["km_recorridos_total"] == 19.0
    assert resumen["fallas_por_tipo"]["motor"] == 1
    assert resumen["fallas_por_tipo"]["electrico"] == 2
    # VEH-001 estaba en riesgo en la 1.a ventana pero ya no en la ultima
    assert resumen["vehiculos_en_riesgo"] == ["VEH-002"]


def test_resumen_de_kpis_sin_filas_devuelve_ceros():
    resumen = resumir_kpis([])
    assert resumen["vehiculos"] == 0
    assert resumen["km_flota_acumulados"] == 0
    assert resumen["vehiculos_en_riesgo"] == []


# --- resumir_alertas ---------------------------------------------------------
def test_resumen_de_alertas_cuenta_por_motivo_y_vehiculo():
    filas = (
        [{"vehiculo_id": "VEH-003", "motivo_alerta": "MANTENIMIENTO_VENCIDO"}] * 3
        + [{"vehiculo_id": "VEH-004", "motivo_alerta": "PARAMETRO_CRITICO"}]
    )
    resumen = resumir_alertas(filas)
    assert resumen["total_alertas"] == 4
    assert resumen["por_motivo"] == {"MANTENIMIENTO_VENCIDO": 3, "PARAMETRO_CRITICO": 1}
    assert resumen["top_vehiculos"][0] == ["VEH-003", 3]


def test_resumen_de_alertas_sin_filas():
    assert resumir_alertas([])["total_alertas"] == 0


# --- reporte -----------------------------------------------------------------
def armar_reporte():
    calidad = evaluar_calidad([fila(1), fila(2)])
    kpis = resumir_kpis([kpi("VEH-001", datetime(2026, 10, 29, 5, 10), 1000.0, 5.0, 1)])
    alertas = resumir_alertas([{"vehiculo_id": "VEH-001", "motivo_alerta": "PARAMETRO_CRITICO"}])
    return construir_reporte(calidad, kpis, alertas, "2026-11-02 10:00:00")


def test_el_reporte_incluye_las_tres_secciones():
    texto = armar_reporte()
    assert "## Calidad de datos" in texto
    assert "## KPIs" in texto
    assert "## Alertas" in texto
    assert "APROBADA" in texto
    assert "VEH-001" in texto


def test_guardar_reporte_crea_el_archivo_con_fecha_en_el_nombre(tmp_path):
    ruta = guardar_reporte(armar_reporte(), str(tmp_path), ahora=datetime(2026, 11, 2, 10, 0, 0))
    assert ruta.endswith("reporte_20261102_100000.md")
    assert os.path.exists(ruta)
    with open(ruta, encoding="utf-8") as archivo:
        assert archivo.read().startswith("# Reporte de control de la flota")
