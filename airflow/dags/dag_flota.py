"""DAG de control de la flota - RapidGo Variante 8 (Mantenimiento de flota).

Spark escribe el Data Lake de forma continua (streaming). Este DAG es el
control por lotes que se ejecuta cuando se quiere revisar ese Data Lake:

    verificar_fuentes -> calidad_datos -> resumen_kpis ----+
                                      \\-> resumen_alertas -+-> generar_reporte

- retries: cada tarea se reintenta 2 veces, con 20 segundos de espera.
- XCom: cada tarea devuelve un diccionario que la siguiente recibe como argumento.
- Fallo controlado: si la variable de Airflow "fallo_controlado" vale "si",
  la tarea calidad_datos falla a proposito (prueba de recuperacion).
"""
import os
from datetime import datetime, timedelta

from airflow.sdk import Variable, dag, task

import calidad_flota as cf

DATALAKE = os.environ.get("DATALAKE", "/opt/airflow/datalake")


def _fallo_controlado_activo():
    """True si la variable de Airflow fallo_controlado vale 'si'."""
    try:
        return Variable.get("fallo_controlado") == "si"
    except Exception:  # la variable no existe todavia: no hay fallo controlado
        return False


@dag(
    dag_id="rapidgo_v8_control_flota",
    description="Calidad, resumen y reporte sobre el Data Lake de la flota",
    schedule=None,  # se ejecuta a mano (Trigger) cuando se quiere revisar el Data Lake
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=20)},
    tags=["rapidgo", "variante-8"],
)
def control_flota():

    @task
    def verificar_fuentes() -> dict:
        """Comprueba que raw, processed y curated tengan archivos Parquet."""
        return cf.verificar_fuentes(DATALAKE)

    @task
    def calidad_datos(fuentes: dict) -> dict:
        """Revisa la calidad de processed. Si no se aprueba, falla y frena el reporte."""
        if _fallo_controlado_activo():
            raise RuntimeError("FALLO CONTROLADO: la variable fallo_controlado vale 'si' "
                               "(prueba de recuperacion)")
        resultado = cf.evaluar_calidad(cf.leer_parquet(os.path.join(DATALAKE, "processed")))
        if not resultado["aprobado"]:
            raise ValueError("Calidad rechazada: " + "; ".join(resultado["problemas"]))
        return resultado

    @task
    def resumen_kpis(calidad: dict) -> dict:
        """Resume los KPIs por ventana (recibe calidad para esperar a que se apruebe)."""
        return cf.resumir_kpis(cf.leer_parquet(os.path.join(DATALAKE, "curated/kpi_vehiculo_ventana")))

    @task
    def resumen_alertas(calidad: dict) -> dict:
        """Cuenta las alertas por motivo y por vehiculo."""
        return cf.resumir_alertas(cf.leer_parquet(os.path.join(DATALAKE, "curated/alertas")))

    @task
    def generar_reporte(calidad: dict, kpis: dict, alertas: dict) -> str:
        """Escribe el reporte en datalake/curated/reportes y devuelve su ruta."""
        texto = cf.construir_reporte(calidad, kpis, alertas,
                                     datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return cf.guardar_reporte(texto, DATALAKE)

    fuentes = verificar_fuentes()
    calidad = calidad_datos(fuentes)
    kpis = resumen_kpis(calidad)
    alertas = resumen_alertas(calidad)
    generar_reporte(calidad, kpis, alertas)


flota_dag = control_flota()
