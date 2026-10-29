"""Producer de telemetria de flota - RapidGo Variante 8.

Envia un evento por vehiculo cada `--intervalo` segundos al topic de Kafka.
La key del mensaje es el vehiculo_id, asi todas las lecturas de un vehiculo
caen en la misma particion y conservan su orden (ver ADR-001).

Ejemplos (desde la raiz del repositorio):
    python producer/producer_flota.py --dry-run --duracion 5 --intervalo 1
    python producer/producer_flota.py --intervalo 10 --duracion 600
"""
import argparse
import json
import random
import sys
import time
from collections import Counter

from generador import crear_flota, siguiente_evento


def leer_argumentos():
    p = argparse.ArgumentParser(description="Producer de telemetria de flota")
    p.add_argument("--bootstrap", default="localhost:9092",
                   help="direccion de Kafka visible desde tu PC (por defecto localhost:9092)")
    p.add_argument("--topic", default="rapidgo.flota.v8")
    p.add_argument("--vehiculos", type=int, default=12)
    p.add_argument("--intervalo", type=float, default=10.0,
                   help="segundos entre lecturas de un mismo vehiculo")
    p.add_argument("--duracion", type=float, default=0,
                   help="segundos que corre el producer (0 = hasta Ctrl+C)")
    p.add_argument("--semilla", type=int, default=None,
                   help="semilla para repetir exactamente la misma simulacion")
    p.add_argument("--dry-run", action="store_true",
                   help="imprime los eventos en pantalla sin enviarlos a Kafka")
    return p.parse_args()


def main():
    args = leer_argumentos()
    rng = random.Random(args.semilla)
    flota = crear_flota(args.vehiculos, rng)

    productor = None
    if not args.dry_run:
        from confluent_kafka import Producer  # se importa aqui para poder usar --dry-run sin la libreria
        productor = Producer({"bootstrap.servers": args.bootstrap})

    por_particion = Counter()
    errores = []

    def al_entregar(error, mensaje):
        if error is not None:
            errores.append(str(error))
        else:
            por_particion[mensaje.partition()] += 1

    enviados = 0
    inicio = time.time()
    print("Producer iniciado. Topic: %s | vehiculos: %d | intervalo: %ss"
          % (args.topic, len(flota), args.intervalo))
    try:
        while True:
            for vehiculo in flota:
                evento = siguiente_evento(vehiculo, rng, intervalo_s=args.intervalo)
                valor = json.dumps(evento)
                if productor is None:
                    print(valor)
                else:
                    productor.produce(args.topic,
                                      key=evento["vehiculo_id"].encode("utf-8"),
                                      value=valor.encode("utf-8"),
                                      callback=al_entregar)
                enviados += 1
            if productor is not None:
                productor.poll(0)
            if args.duracion and time.time() - inicio >= args.duracion:
                break
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\nDetenido con Ctrl+C")
    finally:
        if productor is not None:
            productor.flush(15)

    print("Eventos generados: %d" % enviados)
    if productor is not None:
        print("Entregados por particion: %s" % dict(sorted(por_particion.items())))
        print("Errores de entrega: %d" % len(errores))
        if errores:
            print("Primer error: %s" % errores[0])
            sys.exit(1)


if __name__ == "__main__":
    main()
