# adls_landing: ingesta ADLS (landing) -> Bronze con Auto Loader

Bundle con un job de Databricks que ingesta las tablas de cobranzas desde landing hacia Bronze.

- **Job:** `ingesta_landing_bronze`, un task por tabla (`cuotas`, `pagos`, `gestiones_cobranza`) en paralelo.
- **Notebook:** un único notebook parametrizado, [src/ingesta_autoloader.py](src/ingesta_autoloader.py).
- **Schedule:** todos los días a las 12:00 a.m., zona `America/Lima`.
- **Cómputo:** serverless.
- **Origen:** `<landing_path>/<tabla>/`, Parquet particionado por `año/mes/día`.
- **Checkpoint y schema de Auto Loader:** `<checkpoint_path>/<tabla>/{checkpoint,schema}`.
- **Destino:** `<catalog>.<schema>.<tabla>`. El notebook crea el schema si no existe.

## Requisitos

Ejecutar antes el setup de Unity Catalog ([src/setup/](../../setup/README.md)): crea las external locations
`azure_adls_landing_location` (lectura) y `azure_adls_checkpoint_location` (escritura) que este job usa.

## Variables ([databricks.yml](databricks.yml))

| Variable | Valor actual |
|---|---|
| `catalog` | `bronze_dev` |
| `schema` | `cobranzas` |
| `landing_path` | `abfss://landing@dbwizarduniquelab.dfs.core.windows.net/cobranzas` |
| `checkpoint_path` | `abfss://checkpoint@dbwizarduniquelab.dfs.core.windows.net/autoloader/cobranzas` |

## Desplegar (sin ejecutar)

```powershell
$perfil = "giancarlo92"
cd src\bronze\adls_landing
databricks bundle validate -p $perfil
databricks bundle deploy -p $perfil
```

El workspace lo define el perfil (`$perfil`, un perfil de `~/.databrickscfg`); en otro Databricks solo cambian el perfil y, si aplica, las variables del target.

El target `dev` no usa `mode: development` a propósito: en ese modo el schedule se despliega pausado y el job se
renombra con el prefijo del usuario.

## Pausar y reanudar el schedule

Un job desplegado con un bundle queda en modo `UI_LOCKED`: la UI es de solo lectura y no permite pausarlo desde ahí.
Hay dos formas de hacerlo.

### Opción 1 (recomendada): por código

Es la que perdura, porque el YAML es la fuente de verdad. En
[resources/ingesta_landing.job.yml](resources/ingesta_landing.job.yml) cambia `pause_status` y redespliega:

```yaml
schedule:
  pause_status: PAUSED      # pausar
  # pause_status: UNPAUSED  # reanudar
```

```powershell
$perfil = "giancarlo92"
cd src\bronze\adls_landing
databricks bundle deploy -p $perfil
```

### Opción 2: directo con la API (rápida, temporal)

No toca el YAML. Cualquier `bundle deploy` posterior vuelve a aplicar el `pause_status` que diga el YAML.

Ajusta `$perfil` a tu perfil de `~/.databrickscfg` y pega el bloque tal cual:

```powershell
$perfil = "giancarlo92"
$id = (databricks jobs list --name ingesta_landing_bronze -p $perfil -o json | ConvertFrom-Json)[0].job_id
$f  = Join-Path $env:TEMP "schedule_job.json"

# Pausar
@"
{"job_id": $id, "new_settings": {"schedule": {"quartz_cron_expression": "0 0 0 * * ?", "timezone_id": "America/Lima", "pause_status": "PAUSED"}}}
"@ | Set-Content $f -Encoding ascii
databricks jobs update --json "@$f" -p $perfil

# Reanudar
@"
{"job_id": $id, "new_settings": {"schedule": {"quartz_cron_expression": "0 0 0 * * ?", "timezone_id": "America/Lima", "pause_status": "UNPAUSED"}}}
"@ | Set-Content $f -Encoding ascii
databricks jobs update --json "@$f" -p $perfil

# Comprobar el estado
databricks jobs get $id -p $perfil -o json | Select-String '"pause_status"'
```

`jobs update` reemplaza el bloque `schedule` completo, por eso el JSON repite la cron y la zona horaria.

## Ejecutar manualmente / eliminar

```powershell
$perfil = "giancarlo92"
databricks bundle run ingesta_landing_bronze -p $perfil
databricks bundle destroy -p $perfil
```
