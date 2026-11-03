"""Funciones del DAG de control de la flota - RapidGo Variante 8.

Python puro: no necesita Airflow, Kafka ni Spark, asi que se puede probar con
pytest. El DAG (dag_flota.py) solo conecta estas funciones en un orden.

Lee las capas que escribe el job de Spark en el Data Lake:
    processed/                      eventos validos con las banderas de alerta
    curated/alertas/                eventos que activaron alguna alerta
    curated/kpi_vehiculo_ventana/   KPIs por vehiculo y ventana de 5 minutos
y escribe el reporte en curated/reportes/.
"""
import os
from collections import Counter
from datetime import datetime

# Carpetas del Data Lake que el DAG necesita (relativas a la base del datalake)
FUENTES = ("raw", "processed", "curated/alertas", "curated/kpi_vehiculo_ventana")

# Campos que no pueden venir vacios en processed
CAMPOS_CLAVE = ("event_id", "vehiculo_id", "ts_evento", "kilometraje",
                "km_ultimo_mantenimiento", "temperatura_motor_c")
TEMPERATURA_MIN = 0
TEMPERATURA_MAX = 200
MAX_DUPLICADOS_PCT = 1.0  # se toleran hasta 1 % de eventos repetidos


# --- Tarea 1: verificar fuentes ----------------------------------------------
def contar_archivos_parquet(carpeta):
    """Cuenta los archivos .parquet dentro de una carpeta (0 si no existe)."""
    total = 0
    for _, _, archivos in os.walk(carpeta):
        total += sum(1 for nombre in archivos if nombre.endswith(".parquet"))
    return total


def verificar_fuentes(base, fuentes=FUENTES):
    """Devuelve cuantos archivos Parquet hay en cada capa; falla si alguna esta vacia."""
    resultado = {}
    faltantes = []
    for nombre in fuentes:
        cantidad = contar_archivos_parquet(os.path.join(base, nombre))
        resultado[nombre] = cantidad
        if cantidad == 0:
            faltantes.append(nombre)
    if faltantes:
        raise FileNotFoundError("Sin archivos Parquet en: " + ", ".join(faltantes))
    return resultado


def leer_parquet(carpeta):
    """Lee una carpeta Parquet y la devuelve como lista de diccionarios."""
    import pyarrow.parquet as pq  # aqui adentro para que las pruebas no necesiten pyarrow
    return pq.read_table(carpeta).to_pylist()


# --- Tarea 2: calidad de datos -----------------------------------------------
def evaluar_calidad(filas):
    """Revisa la capa processed y devuelve metricas y si la calidad fue aprobada."""
    total = len(filas)
    con_nulos = sum(1 for f in filas if any(f.get(c) is None for c in CAMPOS_CLAVE))
    ids = [f["event_id"] for f in filas if f.get("event_id") is not None]
    duplicados = len(ids) - len(set(ids))
    duplicados_pct = round(100.0 * duplicados / total, 2) if total else 0.0
    fuera_de_rango = sum(
        1 for f in filas
        if f.get("temperatura_motor_c") is not None
        and not TEMPERATURA_MIN <= f["temperatura_motor_c"] <= TEMPERATURA_MAX)
    km_inconsistentes = sum(
        1 for f in filas
        if f.get("kilometraje") is not None
        and f.get("km_ultimo_mantenimiento") is not None
        and f["km_ultimo_mantenimiento"] > f["kilometraje"])

    problemas = []
    if total == 0:
        problemas.append("la capa processed no tiene filas")
    if con_nulos:
        problemas.append("%d filas con campos clave vacios" % con_nulos)
    if duplicados_pct > MAX_DUPLICADOS_PCT:
        problemas.append("%.2f %% de eventos duplicados (maximo %.1f %%)"
                         % (duplicados_pct, MAX_DUPLICADOS_PCT))
    if fuera_de_rango:
        problemas.append("%d filas con temperatura fuera de rango" % fuera_de_rango)
    if km_inconsistentes:
        problemas.append("%d filas con km_ultimo_mantenimiento mayor que kilometraje"
                         % km_inconsistentes)

    return {
        "total_filas": total,
        "filas_con_nulos": con_nulos,
        "duplicados": duplicados,
        "duplicados_pct": duplicados_pct,
        "temperatura_fuera_de_rango": fuera_de_rango,
        "km_inconsistentes": km_inconsistentes,
        "problemas": problemas,
        "aprobado": not problemas,
    }


# --- Tarea 3: resumen de KPIs ------------------------------------------------
TIPOS_FALLA = (("motor", "fallas_motor"), ("lubricacion", "fallas_lubricacion"),
               ("electrico", "fallas_electrico"), ("combustible", "fallas_combustible"))


def resumir_kpis(filas):
    """Resume la tabla de KPIs por vehiculo y ventana."""
    km_maximo = {}
    ultima_fila = {}
    for f in filas:
        vehiculo = f["vehiculo_id"]
        km_maximo[vehiculo] = max(km_maximo.get(vehiculo, 0), f.get("km_acumulados") or 0)
        if vehiculo not in ultima_fila or f["ventana_inicio"] > ultima_fila[vehiculo]["ventana_inicio"]:
            ultima_fila[vehiculo] = f
    return {
        "ventanas": len({str(f["ventana_inicio"]) for f in filas}),
        "vehiculos": len(km_maximo),
        "km_flota_acumulados": round(sum(km_maximo.values()), 2),
        "km_recorridos_total": round(sum(f.get("km_recorridos_ventana") or 0 for f in filas), 2),
        "fallas_por_tipo": {tipo: sum(f.get(col) or 0 for f in filas) for tipo, col in TIPOS_FALLA},
        "vehiculos_en_riesgo": sorted(v for v, f in ultima_fila.items() if f.get("en_riesgo")),
    }


# --- Tarea 4: resumen de alertas ---------------------------------------------
def resumir_alertas(filas):
    """Cuenta las alertas por motivo y por vehiculo."""
    por_motivo = Counter(f["motivo_alerta"] for f in filas)
    por_vehiculo = Counter(f["vehiculo_id"] for f in filas)
    return {
        "total_alertas": len(filas),
        "por_motivo": dict(por_motivo),
        "top_vehiculos": [[vehiculo, cantidad] for vehiculo, cantidad in por_vehiculo.most_common(5)],
    }


# --- Tarea 5: reporte --------------------------------------------------------
def construir_reporte(calidad, kpis, alertas, generado_en):
    """Arma el reporte en Markdown con los resultados de las tareas anteriores."""
    lineas = [
        "# Reporte de control de la flota - Variante 8",
        "",
        "Generado: %s" % generado_en,
        "",
        "## Calidad de datos",
        "- Filas revisadas en processed: %d" % calidad["total_filas"],
        "- Filas con campos clave vacios: %d" % calidad["filas_con_nulos"],
        "- Eventos duplicados: %d (%.2f %%)" % (calidad["duplicados"], calidad["duplicados_pct"]),
        "- Temperaturas fuera de rango: %d" % calidad["temperatura_fuera_de_rango"],
        "- Resultado: %s" % ("APROBADA" if calidad["aprobado"] else "RECHAZADA"),
        "",
        "## KPIs",
        "- Ventanas cerradas: %d" % kpis["ventanas"],
        "- Vehiculos: %d" % kpis["vehiculos"],
        "- Km acumulados de la flota: %s" % kpis["km_flota_acumulados"],
        "- Fallas por tipo: %s" % ", ".join("%s=%d" % (t, n) for t, n in kpis["fallas_por_tipo"].items()),
        "- Vehiculos en riesgo: %s" % (", ".join(kpis["vehiculos_en_riesgo"]) or "ninguno"),
        "",
        "## Alertas",
        "- Total de eventos con alerta: %d" % alertas["total_alertas"],
    ]
    for motivo, cantidad in sorted(alertas["por_motivo"].items()):
        lineas.append("- %s: %d" % (motivo, cantidad))
    if alertas["top_vehiculos"]:
        lineas.append("- Vehiculos con mas alertas: %s"
                      % ", ".join("%s (%d)" % (v, n) for v, n in alertas["top_vehiculos"]))
    return "\n".join(lineas) + "\n"


def guardar_reporte(texto, base, ahora=None):
    """Guarda el reporte en curated/reportes y devuelve la ruta del archivo."""
    ahora = ahora or datetime.now()
    carpeta = os.path.join(base, "curated", "reportes")
    os.makedirs(carpeta, exist_ok=True)
    ruta = os.path.join(carpeta, "reporte_%s.md" % ahora.strftime("%Y%m%d_%H%M%S"))
    with open(ruta, "w", encoding="utf-8") as archivo:
        archivo.write(texto)
    return ruta
