#!/usr/bin/env python3
import argparse
import os
import sys

from databricks.sdk import WorkspaceClient

parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--namespace", required=True, help="namespace de Event Hubs, sin .servicebus.windows.net")
parser.add_argument("--perfil", required=True, help="perfil de ~/.databrickscfg")
parser.add_argument("--scope", default="wizardbank")
args = parser.parse_args()

connection_string = os.environ.get("WIZARDBANK_EVENTHUB_CONNECTION_STRING")
if not connection_string:
    sys.exit("Falta la variable WIZARDBANK_EVENTHUB_CONNECTION_STRING")

w = WorkspaceClient(profile=args.perfil)
w.secrets.put_secret(scope=args.scope, key="eventhub-namespace", string_value=args.namespace)
w.secrets.put_secret(scope=args.scope, key="eventhub-connection-string", string_value=connection_string)
print(f"Secretos en '{args.scope}': {sorted(s.key for s in w.secrets.list_secrets(scope=args.scope))}")
