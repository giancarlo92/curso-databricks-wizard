# telemetria_app: Event Hubs Capture -> Bronze

Job serverless que ingesta con Auto Loader los `.avro` que Event Hubs Capture deja en ADLS y los escribe en
`bronze_dev.telemetria_app.eventos_app`. Cada ejecución procesa lo pendiente (`availableNow`) y termina.

- Columnas del evento en `string`, `_metadata` (JSON con tópico, partición, offset, sequence_number, hora y archivo de
  origen) y `_ingestion_ts` (`timestamp`, cuándo llegó a Bronze).
- Requiere las external locations de [src/setup/](../../setup/README.md) y Capture activo hacia `landing`.
- Variables (rutas, catálogo, schema, tabla) en [databricks.yml](databricks.yml).

```powershell
$perfil = "giancarlo92"
cd src\bronze\telemetria_app
databricks bundle deploy -p $perfil                      # despliega, con la programación pausada
databricks bundle run ingesta_eventos_app -p $perfil     # ejecución manual
```
