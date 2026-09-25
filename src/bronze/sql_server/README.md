# sql_server: Lakeflow Connect (Azure SQL) -> Bronze

Pipeline query-based que copia las 8 tablas de `lending` de Azure SQL a `bronze_dev.lending`, y un job diario que lo ejecuta
(pausado). `clientes`, `desembolsos`, `ofertas_preaprobadas` y `solicitudes_prestamo` son incrementales (PK + cursor
`fecha_actualizacion`); `campanias`, `paises`, `productos_prestamo` y `tipos_cambio` se recargan completas.

Variables (conexión, base de origen, catálogo, schema) en [databricks.yml](databricks.yml).

## 1. Conexión y catálogo federado (una sola vez por workspace)

El pipeline usa la conexión de Unity Catalog `wizardbank_sqlserver`. El usuario y la contraseña son los de Azure SQL
(`sqladminwizard`); la contraseña va solo como variable de entorno del proceso, no se guarda en archivos.

```powershell
$perfil = "giancarlo92"
$env:WIZARDBANK_SQL_PASSWORD = "<password de Azure SQL>"
databricks connections create --json "{`"name`":`"wizardbank_sqlserver`",`"connection_type`":`"SQLSERVER`",`"options`":{`"host`":`"dbwizardunique-lab-sql.database.windows.net`",`"port`":`"1433`",`"trustServerCertificate`":`"true`",`"user`":`"sqladminwizard`",`"password`":`"$env:WIZARDBANK_SQL_PASSWORD`"}}" -p $perfil

# Lakehouse Federation (consultas directas a Azure SQL desde Databricks)
databricks catalogs create wizardbank_federado --connection-name wizardbank_sqlserver --json '{\"options\":{\"database\":\"dbwizardunique-lab-db\"}}' -p $perfil
```

El notebook del lab (JDBC contra Azure SQL con el secret scope `wizardbank`, y Event Hubs con Capture + Auto Loader) está en
[src/notebooks/pruebas_azure_db.py](../../notebooks/pruebas_azure_db.py).

## 2. Despliegue y ejecución

```powershell
cd src\bronze\sql_server
databricks bundle deploy -p $perfil
databricks bundle run pl_wizardbank_bronze_query_job -p $perfil
```

Si el pipeline y el job ya existen en el workspace (creados desde la UI), antes del deploy hay que enlazarlos para no
duplicarlos ni perder la propiedad de las tablas:

```powershell
databricks bundle deployment bind pl_wizardbank_bronze_query <pipeline_id> -p $perfil --auto-approve
databricks bundle deployment bind pl_wizardbank_bronze_query_job <job_id> -p $perfil --auto-approve
```

## Nota: auto-pausa de Azure SQL

La base es serverless y se pausa por inactividad. Si el pipeline arranca con la base pausada, `clientes` falla con
`QUERY_BASED_CONNECTOR_SOURCE_API_ERROR ... GET_TABLE_SCHEMA` (error no reintentable). Basta una consulta previa para
despertarla y volver a ejecutar, por ejemplo `SELECT 1 FROM wizardbank_federado.lending.paises`.
