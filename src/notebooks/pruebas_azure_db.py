# Databricks notebook source
from pyspark.sql import functions as F

# Credenciales desde Key Vault vía Secret Scope — nunca hardcodeadas
jdbc_url = dbutils.secrets.get("wizardbank", "jdbc-url")
usuario  = dbutils.secrets.get("wizardbank", "jdbc-user")
password = dbutils.secrets.get("wizardbank", "jdbc-password")


# COMMAND ----------

print(jdbc_url[:-1])
print(usuario[:-1])
print(password[:-1])

# COMMAND ----------

# productos_prestamo tiene 9 filas: incremental sería absurdo
df_productos = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "lending.productos_prestamo")
    .option("user", usuario)
    .option("password", password)
    .load()
)


# COMMAND ----------

df_productos.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS bronze_dev;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_dev.wizardbank;

# COMMAND ----------


(df_productos
    .withColumn("_ingestion_ts",  F.current_timestamp())    # metadata técnica
    .withColumn("_source_system", F.lit("wizardbank_azuresql"))
    .write.mode("overwrite").format("delta")
    .saveAsTable("bronze_dev.wizardbank.productos_prestamo")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CONNECTION wizardbank_sqlserver TYPE SQLSERVER
# MAGIC OPTIONS (
# MAGIC   host 'dbwizardunique-lab-sql.database.windows.net',
# MAGIC   port '1433',
# MAGIC   trustServerCertificate 'true',
# MAGIC   user secret('wizardbank', 'jdbc-user'),
# MAGIC   password secret('wizardbank', 'jdbc-password')
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que quedó creada
# MAGIC SHOW CONNECTIONS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE CONNECTION wizardbank_sqlserver;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM bronze_dev.lending.solicitudes_prestamo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reutiliza la MISMA conexión del Paso 1, no hace falta crear otra
# MAGIC CREATE FOREIGN CATALOG wizardbank_federado
# MAGIC USING CONNECTION wizardbank_sqlserver
# MAGIC OPTIONS (database 'dbwizardunique-lab-db');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Ya se puede hacer SQL normal contra el origen, en tiempo real
# MAGIC SELECT top_producto.nombre_producto, COUNT(*) AS solicitudes
# MAGIC FROM wizardbank_federado.lending.solicitudes_prestamo s
# MAGIC JOIN wizardbank_federado.lending.productos_prestamo top_producto
# MAGIC   ON s.id_producto = top_producto.id_producto
# MAGIC GROUP BY top_producto.nombre_producto
# MAGIC ORDER BY solicitudes DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NO FUNCIONA
# MAGIC CREATE STREAMING TABLE bronze_dev.wizardbank.productos_prestamo_federado
# MAGIC AS SELECT * FROM STREAM ingest_from_uc_foreign_catalog(
# MAGIC   source_catalog => 'wizardbank_federado',
# MAGIC   source_schema  => 'lending',
# MAGIC   source_table   => 'productos_prestamo',
# MAGIC   cursor_columns => ARRAY('fecha_actualizacion'),
# MAGIC   primary_keys   => ARRAY('id_producto'),
# MAGIC   scd_type       => 'SCD_TYPE_1'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # AZURE EVENT HUB - CAPTURE + AUTO LOADER
# MAGIC Desde este workspace serverless no se resuelve `*.servicebus.windows.net`, así que el topic no se lee por Kafka.
# MAGIC En su lugar, **Event Hubs Capture** deja los eventos en Avro en ADLS (`landing`) y **Auto Loader** los ingesta a Bronze.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, LongType, DoubleType)

CAPTURE_PATH    = "abfss://landing@dbwizarduniquelab.dfs.core.windows.net/evhns-wizardbank-dfa70747/wizard.lending.eventos-app/"
CHECKPOINT_PATH = "abfss://checkpoint@dbwizarduniquelab.dfs.core.windows.net/autoloader/eventos_app/"
TARGET          = "bronze_dev.wizardbank.eventos_app"

# COMMAND ----------

# Esquema del evento, espejo del contrato definido en la Sesión 8
esquema_sesion = StructType([
    StructField("id_sesion",          StringType()),
    StructField("version_app",        StringType()),
    StructField("sistema_operativo",  StringType()),
    StructField("modelo_dispositivo", StringType()),
])
esquema_contexto = StructType([
    StructField("ubicacion_pantalla", StringType()),
    StructField("posicion",           IntegerType()),
    StructField("tiempo_visible_seg", DoubleType()),
    StructField("monto_simulado",     DoubleType()),   # solo en simulacion_realizada
    StructField("plazo_simulado",     IntegerType()),  # solo en simulacion_realizada
])
esquema_evento = StructType([
    StructField("id_evento",        StringType()),
    StructField("tipo_evento",      StringType()),
    StructField("timestamp_evento", StringType()),     # ISO-8601, se castea después
    StructField("id_cliente",       LongType()),
    StructField("id_oferta",        LongType()),
    StructField("id_pais",          IntegerType()),
    StructField("canal",            StringType()),
    StructField("sesion",           esquema_sesion),
    StructField("contexto",         esquema_contexto),
])

# COMMAND ----------

# Auto Loader sobre los .avro que deja Capture (cada archivo trae el JSON del evento en la columna Body)
df_capture = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "avro")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "schema")
    .load(CAPTURE_PATH)
)

df_capture.printSchema()

# COMMAND ----------

# Decodificar Body (binario -> string -> struct) y aplanar
df_eventos = (df_capture
    .select(
        F.from_json(F.col("Body").cast("string"), esquema_evento).alias("d"),
        F.col("SequenceNumber").alias("_capture_sequence"),
        F.col("Offset").alias("_capture_offset"),
        F.col("EnqueuedTimeUtc").alias("_capture_enqueued_utc"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
    .filter("d.id_evento IS NOT NULL")   # los JSON corruptos irían a cuarentena (Sesión 12)
    .select(
        F.col("d.id_evento").alias("id_evento"),
        F.col("d.tipo_evento").alias("tipo_evento"),
        F.to_timestamp("d.timestamp_evento").alias("timestamp_evento"),
        F.col("d.id_cliente").alias("id_cliente"),
        F.col("d.id_oferta").alias("id_oferta"),
        F.col("d.id_pais").alias("id_pais"),
        F.col("d.canal").alias("canal"),
        F.col("d.sesion.id_sesion").alias("id_sesion"),
        F.col("d.sesion.version_app").alias("version_app"),
        F.col("d.sesion.sistema_operativo").alias("sistema_operativo"),
        F.col("d.sesion.modelo_dispositivo").alias("modelo_dispositivo"),
        F.col("d.contexto.ubicacion_pantalla").alias("ubicacion_pantalla"),
        F.col("d.contexto.posicion").alias("posicion"),
        F.col("d.contexto.tiempo_visible_seg").alias("tiempo_visible_seg"),
        F.col("d.contexto.monto_simulado").alias("monto_simulado"),
        F.col("d.contexto.plazo_simulado").alias("plazo_simulado"),
        "_capture_sequence", "_capture_offset", "_capture_enqueued_utc", "_source_file",
    )
    .withColumn("_ingestion_ts", F.current_timestamp())
)

# COMMAND ----------

# Escribir a Bronze: procesa todo lo pendiente y termina; el checkpoint evita duplicados en re-ejecuciones
(df_eventos
    .writeStream
    .option("checkpointLocation", CHECKPOINT_PATH + "checkpoint")
    .trigger(availableNow=True)
    .toTable(TARGET)
    .awaitTermination()
)

# COMMAND ----------

display(spark.sql(f"SELECT tipo_evento, COUNT(*) AS eventos FROM {TARGET} GROUP BY tipo_evento ORDER BY eventos DESC"))