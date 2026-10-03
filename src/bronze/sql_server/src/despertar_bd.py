# Databricks notebook source
# MAGIC %md
# MAGIC # Despertar Azure SQL
# MAGIC La base es serverless y se pausa por inactividad; la primera conexión tras la pausa expira. Se consulta con reintentos
# MAGIC antes de lanzar el pipeline de Lakeflow, que no reintenta ese error.

# COMMAND ----------

# ruff: noqa: F821
import time

for intento in range(1, 6):
    try:
        spark.sql("SELECT 1 FROM wizardbank_federado.lending.paises LIMIT 1").collect()
        print(f"Azure SQL disponible (intento {intento})")
        break
    except Exception as e:  # noqa: BLE001
        print(f"intento {intento}: {str(e)[:150]}")
        time.sleep(30)
else:
    raise RuntimeError("Azure SQL no respondió")
