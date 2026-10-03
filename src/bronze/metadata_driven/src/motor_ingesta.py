# Databricks notebook source
# MAGIC %md
# MAGIC # Motor de ingesta metadata-driven
# MAGIC Ejecuta los objetos activos de un grupo de `ctl_objetos`. El router `tipo_ingesta` elige el ejecutor
# MAGIC (`AUTOLOADER` o `JDBC_BATCH`). Cada objeto registra su resultado en `ctl_ejecuciones` sin tumbar al resto;
# MAGIC el job falla al final si alguno falló. Las columnas de la fuente van en `string`, con columnas de control estándar
# MAGIC (`_rescued_data`, `_source_file`, `_ingested_at`); `modo_escritura = append` crea la tabla append only.
# MAGIC
# MAGIC Para agregar funcionalidad: escribir `ejecutar_<tipo>(obj, watermark)` que devuelva un DataFrame y registrarlo en
# MAGIC `EJECUTORES`. La bitácora, el aislamiento de fallos y la escritura se reutilizan.

# COMMAND ----------

# ruff: noqa: F821
import concurrent.futures as cf
import uuid
from datetime import UTC, datetime

from pyspark.sql import functions as F

dbutils.widgets.text("grupo", "")
dbutils.widgets.text("catalogo_ctl", "bronze_dev")
dbutils.widgets.text("landing_path", "")      # raíz del container: abfss://landing@<cuenta>.dfs.core.windows.net
dbutils.widgets.text("checkpoint_path", "")   # raíz: abfss://checkpoint@<cuenta>.dfs.core.windows.net/autoloader
dbutils.widgets.text("job_run_id", "")

GRUPO = dbutils.widgets.get("grupo")
CTL = f"{dbutils.widgets.get('catalogo_ctl')}.control"
LANDING = dbutils.widgets.get("landing_path").rstrip("/")
CHECKPOINTS = dbutils.widgets.get("checkpoint_path").rstrip("/")
JOB_RUN_ID = dbutils.widgets.get("job_run_id")

# COMMAND ----------

# Estado y bitácora
def leer_watermark(id_objeto):
    fila = spark.sql(f"SELECT valor_watermark FROM {CTL}.ctl_watermarks WHERE id_objeto = :i",
                     args={"i": id_objeto}).first()
    return fila[0] if fila else None


def actualizar_watermark(id_objeto, valor):
    spark.sql(f"UPDATE {CTL}.ctl_watermarks SET valor_watermark_anterior = valor_watermark, valor_watermark = :v, "
              "fecha_ultima_ejecucion = current_timestamp() WHERE id_objeto = :i", args={"v": valor, "i": id_objeto})


def registrar_inicio(id_ejec, obj, inicio):
    spark.sql(f"INSERT INTO {CTL}.ctl_ejecuciones (id_ejecucion, id_objeto, job_run_id, fecha_inicio, estado) "
              "VALUES (:e, :o, :j, :f, 'Ejecutando')",
              args={"e": id_ejec, "o": obj["id_objeto"], "j": JOB_RUN_ID, "f": inicio})


def registrar_fin(id_ejec, estado, filas, wm_desde, wm_hasta, error=None):
    spark.sql(f"UPDATE {CTL}.ctl_ejecuciones SET fecha_fin = current_timestamp(), estado = :s, filas_escritas = :n, "
              "watermark_desde = :d, watermark_hasta = :h, mensaje_error = :m, "
              "duracion_segundos = CAST(unix_timestamp(current_timestamp()) - unix_timestamp(fecha_inicio) AS INT) "
              "WHERE id_ejecucion = :e",
              args={"s": estado, "n": filas, "d": wm_desde, "h": wm_hasta, "m": error, "e": id_ejec})

# COMMAND ----------

# Ejecutores: devuelven un DataFrame con las columnas de la fuente (y _source_file)
def ejecutar_jdbc_batch(obj, watermark):
    scope = obj["secret_scope"]
    tabla = f'{obj["schema_origen"]}.{obj["objeto_origen"]}'
    col = obj["columna_watermark"]
    if obj["tipo_watermark"] == "TIMESTAMP":
        # El watermark se lee como texto de 7 decimales (datetime2): Spark solo conserva 6 y repetiría la última fila
        filtro = f"WHERE {col} > '{watermark}'" if watermark else ""
        origen = f"(SELECT *, CONVERT(varchar(27), {col}, 121) AS _wm FROM {tabla} {filtro}) AS t"
    elif obj["modo_carga"] == "INCREMENTAL":
        origen = f"(SELECT * FROM {tabla} WHERE {col} > {watermark or 0}) AS t"
    else:
        origen = tabla
    df = (spark.read.format("jdbc")
        .option("url", dbutils.secrets.get(scope, "jdbc-url"))
        .option("dbtable", origen)
        .option("user", dbutils.secrets.get(scope, "jdbc-user"))
        .option("password", dbutils.secrets.get(scope, obj["secret_key"]))
        .load())
    if obj["tipo_watermark"] == "TIMESTAMP":
        df = df.withColumn(col, F.col("_wm")).drop("_wm")
    return df.withColumn("_source_file", F.lit(tabla))


def ejecutar_autoloader(obj, watermark):
    chk = f'{CHECKPOINTS}/{obj["schema_destino"]}/{obj["tabla_destino"]}'
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", obj["formato_origen"])
        .option("cloudFiles.schemaLocation", f"{chk}/schema")
        .load(f'{LANDING}/{obj["objeto_origen"]}/')
        .withColumn("_source_file", F.col("_metadata.file_path")))


# El router: tipo_ingesta decide quién ejecuta
EJECUTORES = {"JDBC_BATCH": ejecutar_jdbc_batch, "AUTOLOADER": ejecutar_autoloader}

# COMMAND ----------

# Escritura común: columnas de la fuente en string + control estándar
def escribir_destino(df, obj, ts):
    destino = f'{obj["catalogo_destino"]}.{obj["schema_destino"]}.{obj["tabla_destino"]}'
    control = ["_rescued_data", "_source_file", "_ingested_at"]
    df = df.withColumn("_ingested_at", F.lit(ts))
    if "_rescued_data" not in df.columns:
        df = df.withColumn("_rescued_data", F.lit(None).cast("string"))
    df = df.select(*[F.col(c).cast("string") for c in df.columns if c not in control], *control)

    if not spark.catalog.tableExists(destino):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {obj['catalogo_destino']}.{obj['schema_destino']}")
        spark.createDataFrame([], df.schema).write.format("delta").saveAsTable(destino)
        if obj["modo_escritura"] == "append":
            spark.sql(f"ALTER TABLE {destino} SET TBLPROPERTIES ('delta.appendOnly' = 'true')")

    if df.isStreaming:
        chk = f'{CHECKPOINTS}/{obj["schema_destino"]}/{obj["tabla_destino"]}/checkpoint'
        q = df.writeStream.option("checkpointLocation", chk).trigger(availableNow=True).toTable(destino)
        q.awaitTermination()
        # numInputRows no es confiable con availableNow: se cuentan las filas con el _ingested_at de esta corrida
        return spark.sql(f"SELECT COUNT(*) FROM {destino} WHERE _ingested_at = :ts", args={"ts": ts}).first()[0], None

    df.write.mode(obj["modo_escritura"]).saveAsTable(destino)
    if obj["modo_carga"] == "FULL":
        return spark.table(destino).count(), None
    # Incremental: filas y watermark de ESTA corrida, leídos de lo que se escribió
    col = f"`{obj['columna_watermark']}`"
    maximo = f"MAX({col})" if obj["tipo_watermark"] == "TIMESTAMP" else f"MAX(CAST({col} AS BIGINT))"
    return tuple(spark.sql(
        f"SELECT COUNT(*), CAST({maximo} AS STRING) FROM {destino} WHERE _ingested_at = :ts", args={"ts": ts}).first())

# COMMAND ----------

def procesar_objeto(obj):
    """Procesa un objeto y registra el resultado. Nunca lanza excepción: un fallo no debe tumbar al resto."""
    id_ejec = str(uuid.uuid4())
    inicio = datetime.now(UTC)
    registrar_inicio(id_ejec, obj, inicio)
    wm_desde = None
    try:
        wm_desde = leer_watermark(obj["id_objeto"])
        df = EJECUTORES[obj["tipo_ingesta"]](obj, wm_desde)
        filas, wm_hasta = escribir_destino(df, obj, inicio)
        if wm_hasta:   # el watermark solo avanza si la escritura confirmó
            actualizar_watermark(obj["id_objeto"], wm_hasta)
        registrar_fin(id_ejec, "Exitoso", filas, wm_desde, wm_hasta)
        return {"objeto": obj["tabla_destino"], "estado": "Exitoso", "filas": filas}
    except Exception as e:  # noqa: BLE001
        registrar_fin(id_ejec, "Fallido", None, wm_desde, None, str(e)[:1000])
        return {"objeto": obj["tabla_destino"], "estado": "Fallido", "error": str(e)[:200]}


def ejecutar_grupo(grupo, paralelismo=4):
    objetos = spark.sql(f"""
        SELECT o.*, f.nombre_fuente, f.secret_scope, f.secret_key
        FROM {CTL}.ctl_objetos o JOIN {CTL}.ctl_fuentes f ON f.id_fuente = o.id_fuente
        WHERE o.es_activo AND f.es_activa AND o.grupo_ejecucion = :g
        ORDER BY o.orden_ejecucion""", args={"g": grupo}).collect()
    with cf.ThreadPoolExecutor(max_workers=paralelismo) as pool:
        resultados = list(pool.map(lambda r: procesar_objeto(r.asDict()), objetos))
    fallidos = [r for r in resultados if r["estado"] == "Fallido"]
    print(f"{len(resultados) - len(fallidos)}/{len(resultados)} objetos OK")
    for r in resultados:
        print(r)
    # El job falla DESPUÉS de intentarlo todo
    if fallidos:
        raise RuntimeError(f"Objetos fallidos: {[f['objeto'] for f in fallidos]}")


ejecutar_grupo(GRUPO)
