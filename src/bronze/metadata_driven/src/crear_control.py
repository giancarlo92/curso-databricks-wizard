# Databricks notebook source
# MAGIC %md
# MAGIC # Schema de control (metadata-driven): catálogo + DDL + configuración
# MAGIC Crea el catálogo (si no existe) y `<catalogo>.control` con `ctl_fuentes`, `ctl_objetos`, `ctl_watermarks` y
# MAGIC `ctl_ejecuciones`, y sincroniza los objetos configurados (MERGE): los watermarks existentes no se tocan.
# MAGIC Dar de alta una tabla nueva es agregar una fila a `objetos`.

# COMMAND ----------

# ruff: noqa: F821
dbutils.widgets.text("catalogo", "bronze_dev")
catalogo = dbutils.widgets.get("catalogo")
ctl = f"{catalogo}.control"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalogo}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ctl}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {ctl}.ctl_fuentes (
    id_fuente     SMALLINT NOT NULL,
    nombre_fuente STRING   NOT NULL,
    tipo_fuente   STRING   NOT NULL,   -- AzureSQL | Files | EventHubs
    secret_scope  STRING,              -- scope de Databricks (NULL si no requiere credencial)
    secret_key    STRING,              -- nombre del secreto, NO el valor
    es_activa     BOOLEAN  NOT NULL
)""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {ctl}.ctl_objetos (
    id_objeto         INT      NOT NULL,
    id_fuente         SMALLINT NOT NULL,
    schema_origen     STRING,
    objeto_origen     STRING   NOT NULL,   -- tabla (JDBC_BATCH) o ruta bajo landing_path (AUTOLOADER)
    tipo_ingesta      STRING   NOT NULL,   -- AUTOLOADER | JDBC_BATCH (el router del motor)
    modo_carga        STRING   NOT NULL,   -- FULL | INCREMENTAL
    columna_watermark STRING,              -- obligatoria en JDBC_BATCH INCREMENTAL
    tipo_watermark    STRING,              -- TIMESTAMP | NUMERIC
    columnas_clave    STRING   NOT NULL,
    formato_origen    STRING,              -- parquet | csv | json | avro
    catalogo_destino  STRING   NOT NULL,
    schema_destino    STRING   NOT NULL,
    tabla_destino     STRING   NOT NULL,
    modo_escritura    STRING   NOT NULL,   -- append (tabla append only) | overwrite
    grupo_ejecucion   STRING   NOT NULL,
    orden_ejecucion   SMALLINT NOT NULL,
    es_activo         BOOLEAN  NOT NULL,
    clasificacion     STRING   NOT NULL,   -- publico | interno | pii
    responsable       STRING
)""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {ctl}.ctl_watermarks (
    id_objeto                INT NOT NULL,
    valor_watermark          STRING,
    valor_watermark_anterior STRING,
    fecha_ultima_ejecucion   TIMESTAMP
)""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {ctl}.ctl_ejecuciones (
    id_ejecucion      STRING    NOT NULL,
    id_objeto         INT       NOT NULL,
    job_run_id        STRING,
    fecha_inicio      TIMESTAMP NOT NULL,
    fecha_fin         TIMESTAMP,
    estado            STRING    NOT NULL,   -- Ejecutando | Exitoso | Fallido
    watermark_desde   STRING,
    watermark_hasta   STRING,
    filas_escritas    BIGINT,
    duracion_segundos INT,
    mensaje_error     STRING
)""")

# COMMAND ----------

fuentes = [
    (1, "wizardbank_azuresql", "AzureSQL", "wizardbank", "jdbc-password", True),
    (2, "landing_adls", "Files", None, None, True),
    (3, "eventhubs_wizardbank", "EventHubs", "wizardbank", "eventhub-connection-string", True),
]

# (id, fuente, schema_origen, objeto_origen, tipo_ingesta, modo_carga, col_wm, tipo_wm, clave, formato,
#  catalogo, schema, tabla, modo_escritura, grupo, orden, activo, clasificacion, responsable)
def autoloader(id_, ruta, formato, schema, tabla, clave, grupo, orden, clasif, resp):
    return (id_, 2 if formato == "parquet" else 3, None, ruta, "AUTOLOADER", "INCREMENTAL", None, None, clave, formato,
            catalogo, schema, tabla, "append", grupo, orden, True, clasif, resp)


def jdbc(id_, tabla, clave, full, grupo, orden, clasif, resp):
    return (id_, 1, "lending", tabla, "JDBC_BATCH", "FULL" if full else "INCREMENTAL",
            None if full else "fecha_actualizacion", None if full else "TIMESTAMP", clave, None,
            catalogo, "lending", tabla, "overwrite" if full else "append", grupo, orden, True, clasif, resp)


objetos = [
    # Cobranzas: parquet en landing (Auto Loader)
    autoloader(1, "cobranzas/cuotas", "parquet", "cobranzas", "cuotas", "id_cuota", "cobranzas", 10, "interno", "cobranzas"),
    autoloader(2, "cobranzas/pagos", "parquet", "cobranzas", "pagos", "id_pago", "cobranzas", 20, "interno", "cobranzas"),
    autoloader(3, "cobranzas/gestiones_cobranza", "parquet", "cobranzas", "gestiones_cobranza", "id_gestion",
               "cobranzas", 30, "interno", "cobranzas"),
    # Azure SQL (lending): catálogos completos y tablas transaccionales incrementales
    jdbc(4, "paises", "id_pais", True, "catalogos", 10, "publico", "equipo-datos"),
    jdbc(5, "productos_prestamo", "id_producto", True, "catalogos", 20, "publico", "equipo-datos"),
    jdbc(6, "campanias", "id_campania", True, "catalogos", 30, "publico", "equipo-datos"),
    jdbc(7, "tipos_cambio", "id_tipo_cambio", True, "catalogos", 40, "publico", "finanzas"),
    jdbc(8, "clientes", "id_cliente", False, "transaccional", 10, "pii", "riesgos"),
    jdbc(9, "ofertas_preaprobadas", "id_oferta", False, "transaccional", 20, "interno", "riesgos"),
    jdbc(10, "solicitudes_prestamo", "id_solicitud", False, "transaccional", 30, "interno", "riesgos"),
    jdbc(11, "desembolsos", "id_desembolso", False, "transaccional", 40, "interno", "riesgos"),
    # Telemetría: avro de Event Hubs Capture crudo; el JSON del Body se parsea en Silver
    autoloader(12, "evhns-wizardbank-dfa70747/wizard.lending.eventos-app", "avro", "telemetria_app",
               "eventos_app_raw", "Offset", "telemetria", 10, "interno", "producto"),
]

cols_fuentes = ("id_fuente SMALLINT, nombre_fuente STRING, tipo_fuente STRING, secret_scope STRING, "
                "secret_key STRING, es_activa BOOLEAN")
cols_objetos = (
    "id_objeto INT, id_fuente SMALLINT, schema_origen STRING, objeto_origen STRING, tipo_ingesta STRING, "
    "modo_carga STRING, columna_watermark STRING, tipo_watermark STRING, columnas_clave STRING, formato_origen STRING, "
    "catalogo_destino STRING, schema_destino STRING, tabla_destino STRING, modo_escritura STRING, "
    "grupo_ejecucion STRING, orden_ejecucion SMALLINT, es_activo BOOLEAN, clasificacion STRING, responsable STRING"
)

spark.createDataFrame(fuentes, cols_fuentes).createOrReplaceTempView("_fuentes")
spark.createDataFrame(objetos, cols_objetos).createOrReplaceTempView("_objetos")

spark.sql(f"MERGE INTO {ctl}.ctl_fuentes t USING _fuentes s ON t.id_fuente = s.id_fuente "
          "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
spark.sql(f"MERGE INTO {ctl}.ctl_objetos t USING _objetos s ON t.id_objeto = s.id_objeto "
          "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
# Solo los incrementales por JDBC guardan watermark (Auto Loader usa el checkpoint; los FULL no recuerdan nada).
# El estado solo se inicializa; nunca se pisa.
spark.sql(f"MERGE INTO {ctl}.ctl_watermarks t "
          "USING (SELECT id_objeto FROM _objetos WHERE tipo_ingesta = 'JDBC_BATCH' AND modo_carga = 'INCREMENTAL') s "
          "ON t.id_objeto = s.id_objeto WHEN NOT MATCHED THEN INSERT (id_objeto) VALUES (s.id_objeto)")

display(spark.table(f"{ctl}.ctl_objetos").select("id_objeto", "tipo_ingesta", "modo_carga", "tabla_destino",
                                                  "grupo_ejecucion", "clasificacion").orderBy("id_objeto"))
