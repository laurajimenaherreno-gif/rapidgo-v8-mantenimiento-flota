"""Reglas de negocio de la variante 8 (Mantenimiento de flota).

Este modulo es Python puro: no necesita Kafka ni Spark, asi que se puede probar
con pytest. Spark usa estas mismas funciones como UDF, de modo que cada regla
se define en un solo lugar.
"""

# --- Umbrales iniciales (ver docs/kpis-y-alertas.md) -------------------------
INTERVALO_MANTENIMIENTO_KM = 5000    # alerta si se supera este intervalo
FRACCION_RIESGO_MANTENIMIENTO = 0.9  # "en riesgo" desde el 90 % del intervalo
TEMP_CRITICA_C = 110.0               # alerta si la temperatura supera este valor
TEMP_RIESGO_C = 100.0                # "en riesgo" desde esta temperatura

# --- Catalogo de fallas (ver docs/contrato-evento.md) ------------------------
# codigo -> (tipo de falla, severidad)
CATALOGO_FALLAS = {
    "P0217": ("MOTOR", "CRITICA"),
    "P0524": ("LUBRICACION", "CRITICA"),
    "P0300": ("MOTOR", "MEDIA"),
    "P0562": ("ELECTRICO", "MEDIA"),
    "P0171": ("COMBUSTIBLE", "BAJA"),
}

TIPOS_VEHICULO = ("moto", "carro")
TOTAL_VEHICULOS = 12


def km_desde_mantenimiento(kilometraje, km_ultimo_mantenimiento):
    """Kilometros recorridos desde el ultimo mantenimiento."""
    if km_ultimo_mantenimiento > kilometraje:
        raise ValueError("km_ultimo_mantenimiento no puede ser mayor que kilometraje")
    return round(kilometraje - km_ultimo_mantenimiento, 1)


def mantenimiento_vencido(kilometraje, km_ultimo_mantenimiento,
                          intervalo=INTERVALO_MANTENIMIENTO_KM):
    """Alerta 1: el vehiculo supero el intervalo de mantenimiento."""
    return km_desde_mantenimiento(kilometraje, km_ultimo_mantenimiento) > intervalo


def falla_critica(codigo_falla):
    """True si el codigo existe en el catalogo y su severidad es CRITICA."""
    if codigo_falla is None:
        return False
    info = CATALOGO_FALLAS.get(codigo_falla)
    return info is not None and info[1] == "CRITICA"


def parametro_critico(temperatura_motor_c, codigo_falla, limite=TEMP_CRITICA_C):
    """Alerta 2: temperatura por encima del limite o falla de severidad CRITICA."""
    return temperatura_motor_c > limite or falla_critica(codigo_falla)


def vehiculo_en_riesgo(kilometraje, km_ultimo_mantenimiento,
                       temperatura_motor_c, codigo_falla):
    """KPI 'vehiculos en riesgo': cumple al menos una de las tres condiciones."""
    km = km_desde_mantenimiento(kilometraje, km_ultimo_mantenimiento)
    cerca_de_mantenimiento = km >= FRACCION_RIESGO_MANTENIMIENTO * INTERVALO_MANTENIMIENTO_KM
    return (cerca_de_mantenimiento
            or temperatura_motor_c >= TEMP_RIESGO_C
            or falla_critica(codigo_falla))


def clasificar_estado(kilometraje, km_ultimo_mantenimiento,
                      temperatura_motor_c, codigo_falla):
    """Resume el estado del vehiculo. Prioridad: CRITICO > VENCIDO > EN_RIESGO > NORMAL."""
    if parametro_critico(temperatura_motor_c, codigo_falla):
        return "CRITICO"
    if mantenimiento_vencido(kilometraje, km_ultimo_mantenimiento):
        return "VENCIDO"
    if vehiculo_en_riesgo(kilometraje, km_ultimo_mantenimiento,
                          temperatura_motor_c, codigo_falla):
        return "EN_RIESGO"
    return "NORMAL"


# --- Validacion del contrato del evento --------------------------------------
CAMPOS_EVENTO = (
    "event_id", "vehiculo_id", "tipo_vehiculo", "timestamp", "kilometraje",
    "km_ultimo_mantenimiento", "temperatura_motor_c", "codigo_falla", "horas_uso",
)
CAMPOS_OBLIGATORIOS = tuple(c for c in CAMPOS_EVENTO if c != "codigo_falla")


def _es_numero(valor):
    return isinstance(valor, (int, float)) and not isinstance(valor, bool)


def validar_evento(evento):
    """Devuelve la lista de errores del evento (lista vacia = evento valido)."""
    import re
    import uuid
    from datetime import datetime

    errores = []
    for campo in CAMPOS_OBLIGATORIOS:
        if evento.get(campo) is None:
            errores.append("falta el campo obligatorio " + campo)
    if errores:
        return errores

    try:
        uuid.UUID(str(evento["event_id"]))
    except ValueError:
        errores.append("event_id no es un UUID valido")

    coincide = re.fullmatch(r"VEH-(\d{3})", str(evento["vehiculo_id"]))
    if not coincide or not 1 <= int(coincide.group(1)) <= TOTAL_VEHICULOS:
        errores.append("vehiculo_id debe estar entre VEH-001 y VEH-012")

    if evento["tipo_vehiculo"] not in TIPOS_VEHICULO:
        errores.append("tipo_vehiculo debe ser moto o carro")

    try:
        datetime.strptime(str(evento["timestamp"]), "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        errores.append("timestamp debe tener formato 2026-10-19T14:20:05Z")

    for campo in ("kilometraje", "km_ultimo_mantenimiento",
                  "temperatura_motor_c", "horas_uso"):
        if not _es_numero(evento[campo]):
            errores.append(campo + " debe ser un numero")
    if errores:
        return errores

    if evento["kilometraje"] < 0:
        errores.append("kilometraje no puede ser negativo")
    if evento["km_ultimo_mantenimiento"] < 0:
        errores.append("km_ultimo_mantenimiento no puede ser negativo")
    if evento["km_ultimo_mantenimiento"] > evento["kilometraje"]:
        errores.append("km_ultimo_mantenimiento no puede ser mayor que kilometraje")
    if not 0 <= evento["temperatura_motor_c"] <= 200:
        errores.append("temperatura_motor_c debe estar entre 0 y 200")
    if not 0 <= evento["horas_uso"] <= 24:
        errores.append("horas_uso debe estar entre 0 y 24")

    codigo = evento.get("codigo_falla")
    if codigo is not None and codigo not in CATALOGO_FALLAS:
        errores.append("codigo_falla no existe en el catalogo")
    return errores
