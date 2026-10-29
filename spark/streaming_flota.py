"""Spark Structured Streaming - RapidGo Variante 8 (Mantenimiento de flota).

Flujo:
    Kafka (topic rapidgo.flota.v8)
      -> query "detalle"  (foreachBatch): escribe tres capas del Data Lake
            raw        : el mensaje de Kafka tal cual (con particion y offset)
            processed  : evento parseado y transformado, con las alertas calculadas
            curated/alertas : solo los eventos que activan alguna de las 2 alertas
      -> query "kpi"      (streaming): agregacion por ventana de tiempo y vehiculo
            curated/kpi_vehiculo_ventana

Se configura con variables de entorno (ver docker-compose.yml):
    KAFKA_BOOTSTRAP, KAFKA_TOPIC, DATALAKE, VENTANA, WATERMARK, TRIGGER,
    SALIDA (parquet | consola), STARTING_OFFSETS (earliest | latest)
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

import reglas

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.environ.get("KAFKA_TOPIC", "rapidgo.flota.v8")
DATALAKE = os.environ.get("DATALAKE", "/opt/app/datalake")
VENTANA = os.environ.get("VENTANA", "5 minutes")
WATERMARK = os.environ.get("WATERMARK", "1 minute")
TRIGGER = os.environ.get("TRIGGER", "10 seconds")
SALIDA = os.environ.get("SALIDA", "parquet")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")

FORMATO_TIMESTAMP = "yyyy-MM-dd'T'HH:mm:ssX"

# Contrato del evento (docs/contrato-evento.md)
ESQUEMA_EVENTO = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("vehiculo_id", T.StringType()),
    T.StructField("tipo_vehiculo", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("kilometraje", T.DoubleType()),
    T.StructField("km_ultimo_mantenimiento", T.DoubleType()),
    T.StructField("temperatura_motor_c", T.DoubleType()),
    T.StructField("codigo_falla", T.StringType()),
    T.StructField("horas_uso", T.DoubleType()),
])


# --- Las reglas de reglas.py, expuestas a Spark como UDF ---------------------
def _vencido(kilometraje, km_ultimo):
    try:
        return bool(reglas.mantenimiento_vencido(kilometraje, km_ultimo))
    except (TypeError, ValueError):
        return None


def _critico(temperatura, codigo):
    try:
        return bool(reglas.parametro_critico(temperatura, codigo))
    except TypeError:
        return None


def _en_riesgo(kilometraje, km_ultimo, temperatura, codigo):
    try:
        return bool(reglas.vehiculo_en_riesgo(kilometraje, km_ultimo, temperatura, codigo))
    except (TypeError, ValueError):
        return None


udf_vencido = F.udf(_vencido, T.BooleanType())
udf_critico = F.udf(_critico, T.BooleanType())
udf_en_riesgo = F.udf(_en_riesgo, T.BooleanType())


def _desde_catalogo(posicion, valor_por_defecto):
    """Expresion que traduce codigo_falla -> tipo (0) o severidad (1) con el catalogo."""
    expresion = F.lit(valor_por_defecto)
    for codigo, info in reglas.CATALOGO_FALLAS.items():
        expresion = F.when(F.col("codigo_falla") == codigo, F.lit(info[posicion])).otherwise(expresion)
    return expresion


# --- Pasos del pipeline ------------------------------------------------------
def parsear(df):
    """Convierte el JSON del mensaje (columna value) en columnas del contrato."""
    return (df
            .withColumn("evento", F.from_json(F.col("value"), ESQUEMA_EVENTO))
            .select("key", "topic", "partition", "offset", "kafka_timestamp", "evento.*"))


def transformar(df):
    """Transformaciones: 1) timestamp, 2) filtro de validos, 3) km desde mantenimiento,
    4) tipo y severidad de falla, 5) banderas de alerta y riesgo."""
    return (df
            # T1: texto ISO 8601 -> timestamp (try_ no falla si el texto esta mal escrito)
            .withColumn("ts_evento",
                        F.try_to_timestamp(F.col("timestamp"), F.lit(FORMATO_TIMESTAMP)))
            # T2: descartar eventos que no cumplen el contrato
            .filter(F.col("event_id").isNotNull()
                    & F.col("vehiculo_id").isNotNull()
                    & F.col("ts_evento").isNotNull()
                    & F.col("kilometraje").isNotNull()
                    & F.col("km_ultimo_mantenimiento").isNotNull()
                    & F.col("temperatura_motor_c").isNotNull()
                    & F.col("tipo_vehiculo").isin(*reglas.TIPOS_VEHICULO)
                    & (F.col("km_ultimo_mantenimiento") <= F.col("kilometraje")))
            # T3: kilometros recorridos desde el ultimo mantenimiento
            .withColumn("km_desde_mantenimiento",
                        F.round(F.col("kilometraje") - F.col("km_ultimo_mantenimiento"), 1))
            # T4: tipo y severidad de la falla segun el catalogo
            .withColumn("tipo_falla", _desde_catalogo(0, "SIN_FALLA"))
            .withColumn("severidad_falla", _desde_catalogo(1, "NINGUNA"))
            # T5: alertas y riesgo (reglas de reglas.py)
            .withColumn("alerta_mantenimiento_vencido",
                        F.coalesce(udf_vencido("kilometraje", "km_ultimo_mantenimiento"), F.lit(False)))
            .withColumn("alerta_parametro_critico",
                        F.coalesce(udf_critico("temperatura_motor_c", "codigo_falla"), F.lit(False)))
            .withColumn("en_riesgo",
                        F.coalesce(udf_en_riesgo("kilometraje", "km_ultimo_mantenimiento",
                                                 "temperatura_motor_c", "codigo_falla"), F.lit(False))))


def solo_alertas(procesado):
    """Eventos que activan al menos una alerta, con el motivo escrito."""
    motivo = F.concat_ws(
        ",",
        F.when(F.col("alerta_mantenimiento_vencido"), F.lit("MANTENIMIENTO_VENCIDO")),
        F.when(F.col("alerta_parametro_critico"), F.lit("PARAMETRO_CRITICO")))
    return (procesado
            .filter(F.col("alerta_mantenimiento_vencido") | F.col("alerta_parametro_critico"))
            .withColumn("motivo_alerta", motivo))


def agregar_kpis(procesado):
    """Agregacion por ventana de tiempo y vehiculo (KPIs de docs/kpis-y-alertas.md)."""
    def fallas(tipo):
        return F.sum(F.when(F.col("tipo_falla") == tipo, 1).otherwise(0))

    agregado = (procesado
                .withWatermark("ts_evento", WATERMARK)
                .groupBy(F.window("ts_evento", VENTANA).alias("ventana"), F.col("vehiculo_id"))
                .agg(F.count("*").alias("lecturas"),
                     F.max("kilometraje").alias("km_acumulados"),
                     F.round(F.max("kilometraje") - F.min("kilometraje"), 2).alias("km_recorridos_ventana"),
                     F.round(F.avg("temperatura_motor_c"), 1).alias("temp_promedio_c"),
                     F.max("temperatura_motor_c").alias("temp_maxima_c"),
                     fallas("MOTOR").alias("fallas_motor"),
                     fallas("LUBRICACION").alias("fallas_lubricacion"),
                     fallas("ELECTRICO").alias("fallas_electrico"),
                     fallas("COMBUSTIBLE").alias("fallas_combustible"),
                     F.max(F.col("en_riesgo").cast("int")).alias("en_riesgo")))
    return agregado.select(
        F.col("ventana.start").alias("ventana_inicio"),
        F.col("ventana.end").alias("ventana_fin"),
        "vehiculo_id", "lecturas", "km_acumulados", "km_recorridos_ventana",
        "temp_promedio_c", "temp_maxima_c", "fallas_motor", "fallas_lubricacion",
        "fallas_electrico", "fallas_combustible", "en_riesgo")


# --- Escritura del Data Lake -------------------------------------------------
def escribir_lote(lote, lote_id):
    """Se ejecuta por cada micro-lote de Kafka: escribe raw, processed y alertas."""
    if lote.isEmpty():
        return
    lote = lote.withColumn("lote_id", F.lit(lote_id)).persist()
    procesado = transformar(parsear(lote))
    alertas = solo_alertas(procesado)

    if SALIDA == "consola":
        print("--- lote %d: %d mensajes leidos de Kafka ---" % (lote_id, lote.count()))
        lote.select("key", "partition", "offset", "value").show(5, truncate=60)
        procesado.select("vehiculo_id", "km_desde_mantenimiento", "temperatura_motor_c",
                         "tipo_falla", "alerta_mantenimiento_vencido",
                         "alerta_parametro_critico", "en_riesgo").show(5, truncate=False)
        print("alertas en este lote: %d" % alertas.count())
    else:
        lote.write.mode("append").partitionBy("partition").parquet(DATALAKE + "/raw")
        procesado.write.mode("append").parquet(DATALAKE + "/processed")
        alertas.write.mode("append").parquet(DATALAKE + "/curated/alertas")
    lote.unpersist()


def main():
    spark = (SparkSession.builder
             .appName("rapidgo-v8-flota")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    kafka = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP)
             .option("subscribe", TOPIC)
             .option("startingOffsets", STARTING_OFFSETS)
             .option("failOnDataLoss", "false")
             .load()
             .select(F.col("key").cast("string").alias("key"),
                     F.col("value").cast("string").alias("value"),
                     "topic", "partition", "offset",
                     F.col("timestamp").alias("kafka_timestamp")))

    # Query 1: raw / processed / alertas
    detalle = (kafka.writeStream
               .foreachBatch(escribir_lote)
               .option("checkpointLocation", DATALAKE + "/_checkpoints/detalle")
               .trigger(processingTime=TRIGGER)
               .start())

    # Query 2: KPIs por ventana
    kpis = agregar_kpis(transformar(parsear(kafka)))
    if SALIDA == "consola":
        salida_kpi = (kpis.writeStream.outputMode("update").format("console")
                      .option("truncate", "false").option("numRows", 20))
    else:
        salida_kpi = (kpis.writeStream.outputMode("append").format("parquet")
                      .option("path", DATALAKE + "/curated/kpi_vehiculo_ventana"))
    kpi = (salida_kpi
           .option("checkpointLocation", DATALAKE + "/_checkpoints/kpi")
           .trigger(processingTime=TRIGGER)
           .start())

    print("Streaming iniciado | topic=%s | ventana=%s | watermark=%s | salida=%s"
          % (TOPIC, VENTANA, WATERMARK, SALIDA))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
