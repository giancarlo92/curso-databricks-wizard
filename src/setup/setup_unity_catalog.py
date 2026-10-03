#!/usr/bin/env python3
"""Configura en Unity Catalog el acceso al ADLS Gen2 del laboratorio Wizard Bank.

Crea o actualiza (es idempotente):
  1. La storage credential (Azure Service Principal).
  2. La external location de landing (solo lectura).
  3. La external location de checkpoint (lectura y escritura).
Y verifica el acceso desde Databricks a cada ruta.

Solo toca Databricks. No ejecuta Terraform ni Azure CLI y no modifica recursos de Azure.
Toda la configuración sale de variables de entorno / archivo .env (ver wizardbank.env.example).
El client secret se lee de WIZARDBANK_SP_CLIENT_SECRET o se pide por consola; nunca se imprime ni se guarda.
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.service.catalog import AzureServicePrincipal, CredentialPurpose

DEFAULT_ENV_FILE = Path(__file__).with_name("wizardbank.env")


@dataclass(frozen=True)
class Location:
    name: str
    container: str
    read_only: bool
    comment: str


def cargar_env(ruta: Path) -> None:
    """Carga KEY=VALUE de un archivo .env. Lo ya definido en el entorno tiene prioridad."""
    if not ruta.is_file():
        return
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, valor = linea.split("=", 1)
        os.environ.setdefault(clave.strip(), valor.strip().strip('"').strip("'"))


def requerida(nombre: str) -> str:
    valor = os.environ.get(nombre, "").strip()
    if not valor:
        sys.exit(f"Falta la variable {nombre}. Revisa wizardbank.env (plantilla: wizardbank.env.example).")
    return valor


def obtener_secreto() -> str:
    secreto = os.environ.get("WIZARDBANK_SP_CLIENT_SECRET", "").strip()
    if secreto:
        return secreto
    if not sys.stdin.isatty():
        sys.exit("Falta WIZARDBANK_SP_CLIENT_SECRET y no hay terminal interactiva para pedirlo.")
    return getpass.getpass("Client secret del Service Principal (no se muestra): ")


def asegurar_credencial(w: WorkspaceClient, nombre: str, sp: AzureServicePrincipal) -> None:
    comentario = "Storage credential (Azure Service Principal) del ADLS Gen2 del laboratorio Wizard Bank."
    try:
        w.storage_credentials.get(nombre)
    except NotFound:
        w.storage_credentials.create(name=nombre, azure_service_principal=sp, comment=comentario, read_only=False)
        print(f"[credencial] {nombre}: creada")
    else:
        # force: Unity Catalog exige este flag para actualizar una credencial que ya tiene locations dependientes.
        w.storage_credentials.update(
            name=nombre, azure_service_principal=sp, comment=comentario, read_only=False, force=True
        )
        print(f"[credencial] {nombre}: actualizada")


def asegurar_location(w: WorkspaceClient, loc: Location, cuenta: str, credencial: str) -> None:
    url = f"abfss://{loc.container}@{cuenta}.dfs.core.windows.net/"
    # File events desactivados: solo los necesita Auto Loader en modo file notification (requiere roles de
    # Event Grid y Storage Queue en Azure). Con el modo por listado de directorios no hacen falta.
    opciones = {
        "url": url,
        "credential_name": credencial,
        "read_only": loc.read_only,
        "comment": loc.comment,
        "enable_file_events": False,
    }
    try:
        w.external_locations.get(loc.name)
    except NotFound:
        w.external_locations.create(name=loc.name, **opciones)
        accion = "creada"
    else:
        w.external_locations.update(name=loc.name, **opciones)
        accion = "actualizada"
    modo = "solo lectura" if loc.read_only else "lectura/escritura"
    print(f"[location] {loc.name}: {accion} -> {url} ({modo})")


def validar(w: WorkspaceClient, loc: Location, credencial: str) -> bool:
    """Valida desde Unity Catalog que la credencial accede a la location."""
    respuesta = w.credentials.validate_credential(
        credential_name=credencial,
        external_location_name=loc.name,
        purpose=CredentialPurpose.STORAGE,
        read_only=loc.read_only,
    )
    resultados = respuesta.results or []
    fallos = [r for r in resultados if r.result and r.result.value == "FAIL"]
    for r in fallos:
        print(f"  FAIL: {r.message}")
    ok = bool(resultados) and not fallos
    print(f"[validacion] {loc.name}: {'OK' if ok else 'FALLO'} ({len(resultados)} pruebas)")
    return ok


def listar(w: WorkspaceClient, warehouse_id: str, loc: Location, cuenta: str) -> bool:
    """Prueba real de lectura: LIST sobre la ruta, ejecutado en un SQL warehouse."""
    url = f"abfss://{loc.container}@{cuenta}.dfs.core.windows.net/"
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=f"LIST '{url}'", wait_timeout="50s"
    )
    estado = resp.status.state.value if resp.status and resp.status.state else "DESCONOCIDO"
    if estado == "SUCCEEDED":
        filas = resp.result.row_count if resp.result and resp.result.row_count is not None else 0
        print(f"[LIST] {loc.name}: OK ({filas} elementos en la raíz)")
        return True
    detalle = resp.status.error.message if resp.status and resp.status.error else estado
    print(f"[LIST] {loc.name}: FALLO ({detalle})")
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE, help="archivo .env con la configuración")
    args = parser.parse_args()
    cargar_env(args.env_file)

    perfil = requerida("DATABRICKS_CONFIG_PROFILE")
    cuenta = requerida("WIZARDBANK_STORAGE_ACCOUNT")
    credencial = requerida("WIZARDBANK_CREDENTIAL_NAME")
    locations = [
        Location(
            requerida("WIZARDBANK_LANDING_LOCATION_NAME"),
            requerida("WIZARDBANK_LANDING_CONTAINER"),
            True,
            "ADLS landing (solo lectura): origen de la ingesta con Auto Loader.",
        ),
        Location(
            requerida("WIZARDBANK_CHECKPOINT_LOCATION_NAME"),
            requerida("WIZARDBANK_CHECKPOINT_CONTAINER"),
            False,
            "ADLS checkpoint (escritura): checkpoints y schemas de Auto Loader.",
        ),
    ]
    sp = AzureServicePrincipal(
        directory_id=requerida("WIZARDBANK_TENANT_ID"),
        application_id=requerida("WIZARDBANK_SP_CLIENT_ID"),
        client_secret=obtener_secreto(),
    )
    warehouse = os.environ.get("WIZARDBANK_WAREHOUSE_ID", "").strip()

    w = WorkspaceClient(profile=perfil)
    print(f"Workspace: {w.config.host} | usuario: {w.current_user.me().user_name}")

    try:
        asegurar_credencial(w, credencial, sp)
        for loc in locations:
            asegurar_location(w, loc, cuenta, credencial)
    except DatabricksError as error:
        print(f"ERROR creando objetos en Unity Catalog: {error}", file=sys.stderr)
        return 1

    todo_ok = True
    for loc in locations:
        try:
            todo_ok &= validar(w, loc, credencial)
            if warehouse:
                todo_ok &= listar(w, warehouse, loc, cuenta)
        except DatabricksError as error:
            print(f"ERROR verificando {loc.name}: {error}", file=sys.stderr)
            todo_ok = False

    print("\nSetup completo y verificado." if todo_ok else "\nSetup terminado CON FALLOS de verificación.")
    return 0 if todo_ok else 1


if __name__ == "__main__":
    sys.exit(main())
