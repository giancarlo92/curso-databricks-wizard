# Databricks notebook source
# MAGIC %md
# MAGIC # DDL de las tablas Bronze de cobranzas
# MAGIC Crea (si no existen) las tablas destino antes de la ingesta: columnas de la fuente en `string`, columnas de control
# MAGIC estándar y `delta.appendOnly` (solo se insertan filas, nunca UPDATE/DELETE).

# COMMAND ----------

# ruff: noqa: F821
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Columnas de la fuente por tabla (todas string; el casteo se hace en Silver)
TABLAS = {
    "cuotas": ["id_cuota", "numero_credito", "numero_cuota", "fecha_vencimiento", "monto_cuota", "monto_capital",
               "monto_interes", "estado_cuota", "fecha_creacion", "fecha_modificacion"],
    "pagos": ["id_pago", "numero_credito", "numero_cuota", "fecha_pago", "monto_pagado", "medio_pago",
              "fecha_creacion", "fecha_modificacion"],
    "gestiones_cobranza": ["id_gestion", "numero_credito", "fecha_gestion", "tipo_gestion", "resultado",
                           "dias_mora_al_momento", "gestor", "fecha_creacion", "fecha_modificacion"],
}

# Columnas de control estándar en todas las tablas
CONTROL = ["_rescued_data STRING", "_source_file STRING", "_ingested_at TIMESTAMP"]

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
for tabla, columnas in TABLAS.items():
    ddl = ", ".join([f"`{c}` STRING" for c in columnas] + CONTROL)
    spark.sql(f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{tabla}` ({ddl}) "
              "TBLPROPERTIES ('delta.appendOnly' = 'true')")
    print(f"{catalog}.{schema}.{tabla}: ok")
