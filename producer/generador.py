"""Generador de eventos de telemetria de flota (variante 8).

Python puro: no necesita Kafka. El producer (producer_flota.py) usa estas
funciones para crear los eventos y los envia al topic rapidgo.flota.v8.
"""
import random
import uuid
from datetime import datetime, timezone

CODIGOS_FALLA = ("P0217", "P0524", "P0300", "P0562", "P0171")

# Vehiculos que, por diseno de la simulacion, arrancan con el mantenimiento ya
# vencido o muy cerca de vencer, para poder observar alertas y vehiculos en riesgo.
VEHICULOS_VENCIDOS = (3, 8)
VEHICULOS_CERCA_DE_VENCER = (5,)


def crear_flota(cantidad=12, rng=None):
    """Crea el estado inicial de cada vehiculo (VEH-001, VEH-002, ...)."""
    rng = rng or random.Random()
    flota = []
    for numero in range(1, cantidad + 1):
        kilometraje = round(rng.uniform(6000, 20000), 1)
        if numero in VEHICULOS_VENCIDOS:
            recorrido = rng.uniform(5000, 5600)
        elif numero in VEHICULOS_CERCA_DE_VENCER:
            recorrido = rng.uniform(4500, 4900)
        else:
            recorrido = rng.uniform(300, 4000)
        flota.append({
            "vehiculo_id": "VEH-%03d" % numero,
            "tipo_vehiculo": "moto" if numero % 2 else "carro",
            "kilometraje": kilometraje,
            "km_ultimo_mantenimiento": round(kilometraje - recorrido, 1),
            "temperatura": rng.uniform(85, 98),
            "horas_uso": round(rng.uniform(0, 6), 2),
        })
    return flota


def siguiente_evento(vehiculo, rng, ahora=None, intervalo_s=10.0,
                     prob_pico=0.03, prob_falla=0.02):
    """Avanza el estado del vehiculo y devuelve el evento (diccionario)."""
    ahora = ahora or datetime.now(timezone.utc)

    # El kilometraje solo sube.
    vehiculo["kilometraje"] = round(vehiculo["kilometraje"] + rng.uniform(0.02, 0.4), 2)

    # La temperatura oscila alrededor de 92 C; a veces sube a un pico critico.
    temperatura = vehiculo["temperatura"]
    temperatura += rng.uniform(-1.5, 1.5) + (92 - temperatura) * 0.1
    temperatura = min(max(temperatura, 70), 105)
    vehiculo["temperatura"] = temperatura

    codigo_falla = None
    temperatura_evento = temperatura
    if rng.random() < prob_pico:
        temperatura_evento = rng.uniform(111, 125)
        codigo_falla = "P0217"
    elif rng.random() < prob_falla:
        codigo_falla = rng.choice(CODIGOS_FALLA)

    vehiculo["horas_uso"] = round(min(vehiculo["horas_uso"] + intervalo_s / 3600, 24), 3)

    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "vehiculo_id": vehiculo["vehiculo_id"],
        "tipo_vehiculo": vehiculo["tipo_vehiculo"],
        "timestamp": ahora.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "kilometraje": vehiculo["kilometraje"],
        "km_ultimo_mantenimiento": vehiculo["km_ultimo_mantenimiento"],
        "temperatura_motor_c": round(temperatura_evento, 1),
        "codigo_falla": codigo_falla,
        "horas_uso": vehiculo["horas_uso"],
    }
