#!/usr/bin/env python3
"""Genera una carga nueva en las 3 fuentes con los scripts existentes:
  azuresql   -> generar_datos_wizard_bank.py --modo incremental   (clientes nuevos + clientes modificados)
  eventhubs  -> productor_eventos.py                               (eventos nuevos; Capture los deja en ADLS)
  cobranzas  -> generar_datos_cobranzas.py --modo incremental      (SQL Server local; luego hay que correr ADF + SHIR)

Uso:  uv run python src/scripts/generar_carga.py            (todas)
      uv run python src/scripts/generar_carga.py azuresql   (solo las indicadas)
"""
import os
import subprocess
import sys
import time
from pathlib import Path

# ── CAMBIAR ANTES DE EJECUTAR (obligatorias) ──────────────────────────────────────────────────────────────────
EVENTHUB_SAS_KEY_NAME="streaming"
EVENTHUB_SAS_KEY = "<clave de la regla de acceso de Event Hubs>"

# ── Valores del laboratorio (cambiar solo si cambia la infraestructura) ───────────────────────────────────────
SQL_SERVER = "dbwizardunique-lab-sql.database.windows.net"
SQL_DATABASE = "dbwizardunique-lab-db"
SQL_USER = "sqladminwizard"
SQL_PASSWORD = "<password de Azure SQL>"
EVENTHUB_NAMESPACE = "evhns-wizardbank-dfa70747"
EVENTHUB_TOPIC = "wizard.lending.eventos-app"
COBRANZAS_SERVER = "localhost,14333"      # SQL Server en Docker
COBRANZAS_DATABASE = "master"
COBRANZAS_USER = "sa"
COBRANZAS_PASSWORD = "<password del SQL Server local>"

# ── Tamaño de la carga ────────────────────────────────────────────────────────────────────────────────────────
NUEVOS_CLIENTES = 100          # azuresql: clientes nuevos (con sus ofertas, solicitudes y desembolsos)
ACTUALIZAR_CLIENTES = 20       # azuresql: clientes existentes que se modifican
EVENTOS = 200                  # eventhubs: eventos a publicar
NUEVOS_CREDITOS = 25           # cobranzas: créditos nuevos (con sus cuotas)
CUOTAS_PAGAR = 30              # cobranzas: cuotas existentes que pasan a pagadas
CUOTAS_VENCER = 15             # cobranzas: cuotas existentes que pasan a vencidas
# ──────────────────────────────────────────────────────────────────────────────────────────────────────────────

AQUI = Path(__file__).parent
ODBC = "Driver={{ODBC Driver 18 for SQL Server}};Server={};Database={};Uid={};Pwd={};Encrypt=yes;TrustServerCertificate={};"
DSN_AZURE = ODBC.format(f"tcp:{SQL_SERVER},1433", SQL_DATABASE, SQL_USER, SQL_PASSWORD, "no")
DSN_COBRANZAS = ODBC.format(COBRANZAS_SERVER, COBRANZAS_DATABASE, COBRANZAS_USER, COBRANZAS_PASSWORD, "yes")
CONNECTION_STRING = (f"Endpoint=sb://{EVENTHUB_NAMESPACE}.servicebus.windows.net/;"
                     f"SharedAccessKeyName={EVENTHUB_SAS_KEY_NAME};SharedAccessKey={EVENTHUB_SAS_KEY}")

CARGAS = {
    "azuresql": ("generar_datos_wizard_bank.py", ["--destino", "azuresql", "--modo", "incremental", "--dsn", DSN_AZURE,
                 "--nuevos-clientes", str(NUEVOS_CLIENTES), "--actualizar-clientes", str(ACTUALIZAR_CLIENTES)]),
    "eventhubs": ("productor_eventos.py", ["--modo", "historico", "--eventos", str(EVENTOS), "--topic", EVENTHUB_TOPIC,
                  "--dsn", DSN_AZURE, "--bootstrap-servers", f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093",
                  "--connection-string", CONNECTION_STRING]),
    "cobranzas": ("generar_datos_cobranzas.py", ["--destino", "local", "--modo", "incremental", "--dsn", DSN_COBRANZAS,
                  "--nuevos-creditos", str(NUEVOS_CREDITOS), "--cuotas-pagar", str(CUOTAS_PAGAR),
                  "--cuotas-vencer", str(CUOTAS_VENCER)]),
}

def despertar_azure_sql(intentos=6, espera=30):
    """Azure SQL serverless se pausa por inactividad: la primera conexión falla (40613) mientras despierta."""
    import pyodbc
    for intento in range(1, intentos + 1):
        print(f"Conectando a Azure SQL (intento {intento}/{intentos}, puede tardar hasta 60 s si estaba pausada)...", flush=True)
        try:
            pyodbc.connect(DSN_AZURE, timeout=60).close()
            print(f"Azure SQL disponible (intento {intento})", flush=True)
            return
        except pyodbc.Error as e:
            print(f"  aún no responde: {str(e)[:80]}", flush=True)
            time.sleep(espera)
    sys.exit("Azure SQL no respondió")


DESCRIPCION = {
    "azuresql": f"Azure SQL: {NUEVOS_CLIENTES} clientes nuevos (con ofertas, solicitudes y desembolsos) y {ACTUALIZAR_CLIENTES} modificados",
    "eventhubs": f"Event Hubs: {EVENTOS} eventos nuevos",
    "cobranzas": f"Cobranzas (SQL Server local): {NUEVOS_CREDITOS} créditos nuevos, {CUOTAS_PAGAR} cuotas pagadas y {CUOTAS_VENCER} vencidas",
}

if __name__ == "__main__":
    elegidas = sys.argv[1:] or list(CARGAS)
    if desconocidas := [f for f in elegidas if f not in CARGAS]:
        sys.exit(f"Fuente desconocida: {desconocidas}. Opciones: {list(CARGAS)}")
    usadas = {"azuresql": ["SQL_PASSWORD"], "eventhubs": ["SQL_PASSWORD", "EVENTHUB_SAS_KEY"],
              "cobranzas": ["COBRANZAS_PASSWORD", "COBRANZAS_DATABASE"]}
    if pendientes := sorted({n for f in elegidas for n in usadas[f] if globals()[n].startswith("<")}):
        sys.exit(f"Completa estas variables al inicio del script: {pendientes}")

    print("CARGA DE DATOS NUEVOS", flush=True)
    if {"azuresql", "eventhubs"} & set(elegidas):
        print("\n[1] Despertando Azure SQL (si estaba pausada puede tardar 1-3 minutos)", flush=True)
        despertar_azure_sql()

    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1", "PYTHONWARNINGS": "ignore"}
    resumen = {}
    for n, fuente in enumerate(elegidas, start=2):
        script, args = CARGAS[fuente]
        print(f"\n[{n}] {DESCRIPCION[fuente]}", flush=True)
        inicio = time.time()
        ok = subprocess.run([sys.executable, str(AQUI / script), *args], env=env, check=False).returncode == 0
        resumen[fuente] = f"{'OK' if ok else 'FALLÓ'} ({time.time() - inicio:.0f} s)"

    print("\n" + "=" * 60 + "\nRESUMEN")
    for fuente, estado in resumen.items():
        print(f"  {'✔' if estado.startswith('OK') else '✘'} {DESCRIPCION[fuente].split(':')[0]:<26} {estado}")
    if resumen.get("cobranzas", "").startswith("OK"):
        print("\nSiguiente paso: correr ADF + SHIR para que las cuotas, pagos y gestiones lleguen a landing.")
    print("Luego ejecuta el job ingesta_metadata_driven para ingestar todo a Bronze.")
    sys.exit(1 if any(not v.startswith("OK") for v in resumen.values()) else 0)
