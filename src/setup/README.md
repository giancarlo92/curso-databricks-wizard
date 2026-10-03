# Setup de Databricks (Unity Catalog)

Configura el acceso de Databricks al ADLS Gen2 del laboratorio. Solo toca Databricks: no ejecuta Terraform ni
Azure CLI y no modifica recursos de Azure. Es **idempotente**: se puede ejecutar las veces que haga falta.

## Qué crea

| Objeto | Nombre | Detalle |
|---|---|---|
| Storage credential | `azure_adls_wizardbank_credential` | Azure Service Principal, lectura/escritura |
| External location | `azure_adls_landing_location` | `abfss://landing@<cuenta>.dfs.core.windows.net/`, **solo lectura** |
| External location | `azure_adls_checkpoint_location` | `abfss://checkpoint@<cuenta>.dfs.core.windows.net/`, **lectura/escritura** |

Las locations se crean con **file events desactivados**: solo los necesita Auto Loader en modo *file notification* y
exigen roles extra en Azure (EventGrid EventSubscription Contributor, Storage Queue Data Contributor). Con el modo por
listado de directorios, que usa la ingesta, no hacen falta.

Al terminar valida cada location con Unity Catalog y hace un `LIST` real sobre cada ruta desde un SQL warehouse.
Los nombres `smoke` no se usan.

## Ejecutar (un comando)

```powershell
$env:WIZARDBANK_SP_CLIENT_SECRET = '<client secret>'   # o déjalo vacío y el script lo pide sin mostrarlo
uv run python src\setup\setup_unity_catalog.py
```

Termina con código `0` si todo quedó creado y verificado, y `1` si algo falló.

## Variables

Se leen de `src/setup/wizardbank.env` (ignorado por git; plantilla en `wizardbank.env.example`).
Una variable ya definida en el entorno tiene prioridad sobre el archivo.

| Variable | Qué es |
|---|---|
| `DATABRICKS_CONFIG_PROFILE` | Perfil de `~/.databrickscfg` del workspace destino |
| `WIZARDBANK_TENANT_ID` | Tenant (directory) ID del Service Principal |
| `WIZARDBANK_SP_CLIENT_ID` | Client (application) ID del Service Principal |
| `WIZARDBANK_SP_CLIENT_SECRET` | Client secret. **Solo por entorno o prompt, nunca en archivos** |
| `WIZARDBANK_STORAGE_ACCOUNT` | Cuenta de ADLS Gen2 (`dbwizarduniquelab`) |
| `WIZARDBANK_LANDING_CONTAINER` / `WIZARDBANK_CHECKPOINT_CONTAINER` | Contenedores (`landing` / `checkpoint`) |
| `WIZARDBANK_CREDENTIAL_NAME` | Nombre de la credential |
| `WIZARDBANK_LANDING_LOCATION_NAME` / `WIZARDBANK_CHECKPOINT_LOCATION_NAME` | Nombres de las locations |
| `WIZARDBANK_WAREHOUSE_ID` | Opcional: SQL warehouse para la prueba `LIST` |

## Replicar en otro Databricks

1. Instala el CLI y autentica el nuevo workspace: `databricks auth login --host <url> --profile <perfil>`.
2. Copia `wizardbank.env.example` a `wizardbank.env` y completa `DATABRICKS_CONFIG_PROFILE`, tenant, client ID,
   cuenta de storage y (opcional) `WIZARDBANK_WAREHOUSE_ID`.
3. Exporta `WIZARDBANK_SP_CLIENT_SECRET` y ejecuta el comando de arriba.

## Secretos (scope `wizardbank`)

Crea el scope y los secretos `jdbc-url`, `jdbc-user`, `jdbc-password` (Azure SQL) y `eventhub-namespace`. La contraseña de
Azure SQL va por variable de entorno; la connection string de Event Hubs es opcional (Capture + Auto Loader no la usa).

```powershell
$env:WIZARDBANK_SQL_PASSWORD = '<password de Azure SQL>'
$env:WIZARDBANK_EVENTHUB_CONNECTION_STRING = '<connection string>'   # opcional
uv run python src\setup\setup_secrets.py --perfil <perfil>
```

## Requisitos previos (fuera de este script)

- El Service Principal debe tener en Azure el rol **Storage Blob Data Contributor** sobre la cuenta de storage
  (lo asigna Terraform). Sin él, la validación de `checkpoint` falla.
- Debes ser metastore admin o tener `CREATE STORAGE CREDENTIAL` y `CREATE EXTERNAL LOCATION` en el metastore.
