"""Lee las capas del Data Lake y muestra evidencia real de la ejecucion:
conteos, mensajes por particion de Kafka (con sus offsets), alertas y KPIs."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/opt/app/datalake"

spark = (SparkSession.builder.appName("ver-datalake")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def leer(ruta):
    try:
        return spark.read.parquet(BASE + "/" + ruta)
    except Exception:
        print("=== %s: todavia no hay datos ===" % ruta)
        return None


raw = leer("raw")
if raw is not None:
    print("=== raw: %d mensajes ===" % raw.count())
    (raw.groupBy("partition")
        .agg(F.count("*").alias("mensajes"),
             F.min("offset").alias("offset_min"),
             F.max("offset").alias("offset_max"))
        .orderBy("partition").show())
    print("Vehiculos (key) por particion:")
    (raw.groupBy("partition")
        .agg(F.sort_array(F.collect_set("key")).alias("vehiculos"))
        .orderBy("partition").show(truncate=False))

procesado = leer("processed")
if procesado is not None:
    print("=== processed: %d eventos validos ===" % procesado.count())
    procesado.select("vehiculo_id", "ts_evento", "km_desde_mantenimiento",
                     "temperatura_motor_c", "tipo_falla",
                     "alerta_mantenimiento_vencido", "alerta_parametro_critico",
                     "en_riesgo").orderBy(F.col("ts_evento").desc()).show(5, truncate=False)

alertas = leer("curated/alertas")
if alertas is not None:
    print("=== alertas: %d eventos con alerta ===" % alertas.count())
    (alertas.groupBy("motivo_alerta", "vehiculo_id").count()
            .orderBy(F.col("count").desc()).show(10, truncate=False))

kpi = leer("curated/kpi_vehiculo_ventana")
if kpi is not None:
    print("=== kpi_vehiculo_ventana: %d filas (una por vehiculo y ventana cerrada) ===" % kpi.count())
    kpi.orderBy("ventana_inicio", "vehiculo_id").show(12, truncate=False)
