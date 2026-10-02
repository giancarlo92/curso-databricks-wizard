# metadata_driven: ingesta dirigida por metadata

Lab 14. Un motor único lee `<catalogo>.control.ctl_objetos` y ejecuta cada objeto activo según su `tipo_ingesta`
(`AUTOLOADER` o `JDBC_BATCH`), en lugar de una tarea por fuente. Es el único bundle de ingesta que se despliega:
cubre todas las fuentes.

- **Job:** `ingesta_metadata_driven`: `crear_control` (crea el catálogo si no existe, el schema `control` y sincroniza la
  configuración), `despertar_bd` y un task por grupo (`cobranzas`, `catalogos`, `transaccional`, `telemetria`).
  Programación diaria a las 12:00 a.m. (Lima), **pausada**.
- **[src/crear_control.py](src/crear_control.py):** DDL de `ctl_fuentes`, `ctl_objetos`, `ctl_watermarks`, `ctl_ejecuciones`
  y la lista de objetos (MERGE; los watermarks no se pisan). Aquí se da de alta una tabla nueva.
- **[src/motor_ingesta.py](src/motor_ingesta.py):** el motor. Columnas de la fuente en `string` más `_rescued_data`,
  `_source_file`, `_ingested_at`. `modo_escritura = append` crea la tabla append only; `overwrite` es para los catálogos
  completos. Cada objeto registra su resultado en `ctl_ejecuciones` sin tumbar a los demás; el job falla al final si alguno falló.
- **[src/despertar_bd.py](src/despertar_bd.py):** Azure SQL serverless se pausa; consulta con reintentos antes de los grupos JDBC.
- **Fuera del framework:** Lakeflow Connect, streaming/CDC y el parseo del JSON de telemetría (Silver).

## Objetos configurados

| Grupo | Objetos | Origen | Destino |
|---|---|---|---|
| `cobranzas` | `cuotas`, `pagos`, `gestiones_cobranza` | Auto Loader (parquet) `landing/cobranzas/<tabla>` | `<catalogo>.cobranzas.<tabla>` (append only) |
| `catalogos` | `paises`, `productos_prestamo`, `campanias`, `tipos_cambio` | JDBC completo (Azure SQL) | `<catalogo>.lending.<tabla>` (overwrite) |
| `transaccional` | `clientes`, `ofertas_preaprobadas`, `solicitudes_prestamo`, `desembolsos` | JDBC incremental por `fecha_actualizacion` | `<catalogo>.lending.<tabla>` (append only) |
| `telemetria` | `eventos_app_raw` | Auto Loader (avro) de Event Hubs Capture | `<catalogo>.telemetria_app.eventos_app_raw` (append only) |

## Extender

- **Otra tabla:** agregar una fila en `objetos` de `crear_control.py` con `autoloader(...)` o `jdbc(...)`.
- **Otro tipo de ingesta:** escribir `ejecutar_<tipo>(obj, watermark)` en `motor_ingesta.py` (devuelve un DataFrame) y
  registrarlo en `EJECUTORES`. La bitácora, el aislamiento de fallos y la escritura se reutilizan.

## Requisitos

- Setup de Unity Catalog ([src/setup/](../../setup/README.md)): external locations de landing y checkpoint.
- Secretos en el scope `wizardbank` (`setup_secrets.py` del mismo setup).

## Variables ([databricks.yml](databricks.yml))

`catalog`, `landing_path` (raíz del container landing), `checkpoint_path` (raíz de `autoloader`) y `checkpoint_version`.

**Recargar todo con Auto Loader:** los checkpoints viven en `<checkpoint_path>/<checkpoint_version>/<schema>/<tabla>`. Si las tablas
se recrean (por ejemplo en otro workspace) pero el checkpoint ya existe en el storage, Auto Loader no recarga nada. Cambia
`checkpoint_version` (`v1` → `v2`) para empezar de cero, y **vacía o borra antes las tablas destino**: son append only y
se duplicarían las filas.

## Desplegar y ejecutar

```powershell
$perfil = "giancarlo92"
cd src\bronze\metadata_driven
databricks bundle validate -p $perfil
databricks bundle deploy -p $perfil
databricks bundle run ingesta_metadata_driven -p $perfil
```

Un job de bundle queda `UI_LOCKED`: para reanudar el schedule, cambia `pause_status` en
[resources/ingesta_metadata_driven.job.yml](resources/ingesta_metadata_driven.job.yml) y vuelve a desplegar.
