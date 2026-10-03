#!/usr/bin/env python3
"""Crea el secret scope del laboratorio con los secretos de Azure SQL y, opcionalmente, de Event Hubs (idempotente).

Las contraseñas y la connection string llegan solo por variable de entorno, nunca por archivos ni argumentos."""
import argparse
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--perfil", required=True, help="perfil de ~/.databrickscfg")
parser.add_argument("--scope", default="wizardbank")
parser.add_argument("--sql-server", default="dbwizardunique-lab-sql.database.windows.net")
parser.add_argument("--sql-database", default="dbwizardunique-lab-db")
parser.add_argument("--sql-user", default="sqladminwizard")
parser.add_argument("--eventhub-namespace", default="evhns-wizardbank-dfa70747", help="sin .servicebus.windows.net")
args = parser.parse_args()

password = os.environ.get("WIZARDBANK_SQL_PASSWORD")
if not password:
    sys.exit("Falta la variable WIZARDBANK_SQL_PASSWORD")

w = WorkspaceClient(profile=args.perfil)
try:
    w.secrets.create_scope(scope=args.scope)
except ResourceAlreadyExists:
    pass

secretos = {
    "jdbc-url": f"jdbc:sqlserver://{args.sql_server}:1433;database={args.sql_database};encrypt=true;trustServerCertificate=false;",
    "jdbc-user": args.sql_user,
    "jdbc-password": password,
    "eventhub-namespace": args.eventhub_namespace,
}
# Con Capture + Auto Loader la connection string no se usa; solo se guarda si se entrega
if os.environ.get("WIZARDBANK_EVENTHUB_CONNECTION_STRING"):
    secretos["eventhub-connection-string"] = os.environ["WIZARDBANK_EVENTHUB_CONNECTION_STRING"]

for clave, valor in secretos.items():
    w.secrets.put_secret(scope=args.scope, key=clave, string_value=valor)
print(f"Secretos en '{args.scope}': {sorted(s.key for s in w.secrets.list_secrets(scope=args.scope))}")
