# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta landing -> bronze con Auto Loader
# MAGIC Notebook parametrizado: ingesta una tabla de `<landing_path>/<table>/` (Parquet, particionado por año/mes/día)
# MAGIC hacia `<catalog>.<schema>.<table>`. El checkpoint y el schema de Auto Loader viven en `<checkpoint_path>/<table>/`.

# COMMAND ----------

# ruff: noqa: F821
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("table", "")
dbutils.widgets.text("landing_path", "")
dbutils.widgets.text("checkpoint_path", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
landing_path = dbutils.widgets.get("landing_path").rstrip("/")
checkpoint_path = dbutils.widgets.get("checkpoint_path").rstrip("/")

for nombre, valor in [("catalog", catalog), ("schema", schema), ("table", table),
                      ("landing_path", landing_path), ("checkpoint_path", checkpoint_path)]:
    if not valor:
        raise ValueError(f"El parámetro '{nombre}' es obligatorio")

source = f"{landing_path}/{table}/"
schema_location = f"{checkpoint_path}/{table}/schema"
checkpoint_location = f"{checkpoint_path}/{table}/checkpoint"
target = f"`{catalog}`.`{schema}`.`{table}`"

print(f"source={source}\nschema_location={schema_location}\ncheckpoint={checkpoint_location}\ntarget={target}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

from pyspark.sql import functions as F

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    df.writeStream
    .option("checkpointLocation", checkpoint_location)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target)
)
query.awaitTermination()
