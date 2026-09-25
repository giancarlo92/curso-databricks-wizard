# Databricks notebook source
# MAGIC %md
# MAGIC # Telemetría de la app: Capture (avro) -> Bronze

# COMMAND ----------

# ruff: noqa: F821
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

capture_path = dbutils.widgets.get("capture_path")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
target = dbutils.widgets.get("target")   # catalog.schema.table

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target.rsplit('.', 1)[0]}")

# COMMAND ----------

# Contrato del evento; todo en string (el casteo se hace en Silver)
def campos(*nombres):
    return [StructField(n, StringType()) for n in nombres]

esquema_evento = StructType(
    campos("id_evento", "tipo_evento", "timestamp_evento", "id_cliente", "id_oferta", "id_pais", "canal")
    + [StructField("sesion", StructType(campos("id_sesion", "version_app", "sistema_operativo", "modelo_dispositivo"))),
       StructField("contexto", StructType(campos("ubicacion_pantalla", "posicion", "tiempo_visible_seg",
                                                 "monto_simulado", "plazo_simulado")))]
)

# COMMAND ----------

# Capture guarda el tópico y la partición en la ruta: .../<event hub>/<partición>/<año>/...
ruta = "_metadata.file_path"
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "avro")
    .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
    .load(capture_path)
    .select(
        F.from_json(F.col("Body").cast("string"), esquema_evento).alias("d"),
        F.to_json(F.struct(
            F.regexp_extract(ruta, r"/([^/]+)/(\d+)/\d{4}/", 1).alias("topico"),
            F.regexp_extract(ruta, r"/([^/]+)/(\d+)/\d{4}/", 2).alias("particion"),
            F.col("Offset").alias("offset"),
            F.col("SequenceNumber").cast("string").alias("sequence_number"),
            F.col("EnqueuedTimeUtc").alias("enqueued_time_utc"),
            F.col(ruta).alias("archivo_origen"),
        )).alias("_metadata"),
    )
    .select("d.*", "d.sesion.*", "d.contexto.*", "_metadata")
    .drop("sesion", "contexto")
    .withColumn("_ingestion_ts", F.current_timestamp())   # cuándo llegó a Bronze
)

# COMMAND ----------

# Procesa los archivos nuevos de Capture y termina; el checkpoint evita reprocesar
query = (df.writeStream
    .option("checkpointLocation", checkpoint_path + "/checkpoint")
    .trigger(availableNow=True)
    .toTable(target)
)
query.awaitTermination()
print(f"estado: {query.status['message']} | filas ingestadas: {(query.lastProgress or {}).get('numInputRows', 0)}")
