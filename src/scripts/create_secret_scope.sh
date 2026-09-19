
# Scope respaldado en Databricks — suficiente para el curso
databricks secrets create-scope wizardbank --profile giancarlo92

# Cada put-secret pide el valor por stdin o interactivo (nunca como argumento en texto plano)
databricks secrets put-secret wizardbank jdbc-url --profile giancarlo92
# valor: jdbc:sqlserver://dbwizardunique-lab-sql.database.windows.net:1433;database=dbwizardunique-lab-db;encrypt=true;trustServerCertificate=false;

databricks secrets put-secret wizardbank jdbc-user --profile giancarlo92
# valor: sqladminwizard

databricks secrets put-secret wizardbank jdbc-password --profile giancarlo92
# valor: g10nCARLO$2292$Databricks

# Verificar que quedaron las 3 (nunca muestra el valor, solo el nombre)
databricks secrets list-secrets wizardbank --profile giancarlo92



# Desde tu terminal local — el script vive en infra/fuentes/ del repo
pip install pyodbc   # + driver ODBC 18 for SQL Server instalado en el SO


# Comando para ejecutar en Azure y generar los datos en la base de datos SQL Server
$env:PYTHONIOENCODING = 'utf-8'
$env:WIZARD_BANK_DSN = 'Driver={ODBC Driver 17 for SQL Server};Server=tcp:dbwizardunique-lab-sql.database.windows.net,1433;Database=dbwizardunique-lab-db;Uid=sqladminwizard;Pwd=TU_PASSWORD;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

try {
    uv run --extra azuresql python src\scripts\generar_datos_wizard_bank.py `
        --destino azuresql `
        --truncar `
        --dsn $env:WIZARD_BANK_DSN
}
finally {
    Remove-Item Env:WIZARD_BANK_DSN,Env:PYTHONIOENCODING -ErrorAction SilentlyContinue
}
        