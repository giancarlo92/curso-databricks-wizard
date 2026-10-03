# Databricks notebook source
# MAGIC %md
# MAGIC # Despertar Azure SQL
# MAGIC La base es serverless y se pausa por inactividad; la primera conexión tras la pausa expira. Se consulta con
# MAGIC reintentos antes de que el motor lea por JDBC.

# COMMAND ----------

# ruff: noqa: F821
import time

dbutils.widgets.text("secret_scope", "wizardbank")
scope = dbutils.widgets.get("secret_scope")

for intento in range(1, 6):
    try:
        (spark.read.format("jdbc")
            .option("url", dbutils.secrets.get(scope, "jdbc-url"))
            .option("dbtable", "(SELECT 1 AS ok) AS t")
            .option("user", dbutils.secrets.get(scope, "jdbc-user"))
            .option("password", dbutils.secrets.get(scope, "jdbc-password"))
            .load().collect())
        print(f"Azure SQL disponible (intento {intento})")
        break
    except Exception as e:  # noqa: BLE001
        print(f"intento {intento}: {str(e)[:150]}")
        time.sleep(30)
else:
    raise RuntimeError("Azure SQL no respondió")
