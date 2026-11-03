"""Pruebas de la estructura del DAG. Solo corren donde Airflow esta instalado
(el job validar-dag del CI); en los demas lugares se omiten."""
import pytest

pytest.importorskip("airflow.sdk")

import dag_flota  # noqa: E402  (se importa despues del importorskip a proposito)

DAG = dag_flota.flota_dag
TAREAS = {"verificar_fuentes", "calidad_datos", "resumen_kpis",
          "resumen_alertas", "generar_reporte"}


def test_el_dag_se_importa_con_su_identificador():
    assert DAG.dag_id == "rapidgo_v8_control_flota"


def test_el_dag_tiene_al_menos_cinco_tareas():
    assert len(DAG.tasks) >= 5
    assert TAREAS.issubset(DAG.task_dict.keys())


def test_las_dependencias_son_las_esperadas():
    tareas = DAG.task_dict
    assert tareas["verificar_fuentes"].upstream_task_ids == set()
    assert tareas["calidad_datos"].upstream_task_ids == {"verificar_fuentes"}
    assert tareas["resumen_kpis"].upstream_task_ids == {"calidad_datos"}
    assert tareas["resumen_alertas"].upstream_task_ids == {"calidad_datos"}
    assert tareas["generar_reporte"].upstream_task_ids == {
        "calidad_datos", "resumen_kpis", "resumen_alertas"}


def test_todas_las_tareas_tienen_reintentos():
    assert all(tarea.retries >= 1 for tarea in DAG.tasks)
