#!/usr/bin/env python3
"""Genera y carga datos sintéticos de cobranzas en el SQL Server local."""

from __future__ import annotations

import argparse
import csv
import os
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path


N_CREDITOS_DEFAULT = 26_960
PLAZO_MIN, PLAZO_MAX = 6, 36
FECHA_INICIO_CREDITOS = date(2024, 8, 1)
FECHA_FIN_CREDITOS = date(2026, 7, 31)
HOY = date(2026, 8, 15)
SEED = 42
PROB_PAGADA_A_TIEMPO = 0.78
PROB_PAGADA_TARDE = 0.13
MEDIOS_PAGO = ["Transferencia", "Debito Automatico", "Ventanilla", "Agente"]
PESO_MEDIOS_PAGO = [0.45, 0.35, 0.12, 0.08]
TIPOS_GESTION = ["Llamada", "SMS", "Email", "Visita", "Carta Notarial"]
PESO_TIPOS = [0.45, 0.25, 0.15, 0.10, 0.05]
RESULTADOS_GESTION = ["Compromiso de pago", "Sin respuesta", "Rechazo", "Pago inmediato"]
PESO_RESULTADOS = [0.35, 0.40, 0.15, 0.10]
NOMBRES_GESTOR = [
    "Ana Beltrán", "Luis Vega", "Carla Rojas", "Miguel Soto", "Paula Vidal",
    "Jorge Núñez", "Karen Salas", "Diego Paredes", "Rocío Aguirre", "Iván Cornejo",
]
ORDEN_CARGA = ["cuotas", "pagos", "gestiones_cobranza"]


def esquema_de(destino: str) -> str:
    """El flujo local usa el mismo schema que referencia el pipeline de ADF."""
    return "cobranzas"


def sumar_meses(fecha: date, cantidad: int) -> date:
    mes = fecha.month - 1 + cantidad
    anio = fecha.year + mes // 12
    mes = mes % 12 + 1
    return date(anio, mes, min(fecha.day, 28))


def fecha_aleatoria(desde: date, hasta: date) -> date:
    return desde + timedelta(days=random.randint(0, max((hasta - desde).days, 0)))


def generar_credito_historico(numero_credito: int) -> tuple[list[dict], list[dict], list[dict]]:
    cuotas: list[dict] = []
    pagos: list[dict] = []
    gestiones: list[dict] = []
    fecha_desembolso = fecha_aleatoria(FECHA_INICIO_CREDITOS, FECHA_FIN_CREDITOS)
    plazo = random.randint(PLAZO_MIN, PLAZO_MAX)
    monto_cuota = round(random.uniform(150, 2500), 2)
    vencidas: list[date] = []

    for numero_cuota in range(1, plazo + 1):
        vencimiento = sumar_meses(fecha_desembolso, numero_cuota)
        interes = round(monto_cuota * 0.18, 2)
        capital = round(monto_cuota - interes, 2)
        if vencimiento > HOY:
            estado = "Pendiente"
        elif random.random() < PROB_PAGADA_A_TIEMPO + PROB_PAGADA_TARDE:
            estado = "Pagada"
        else:
            estado = "Vencida"
            vencidas.append(vencimiento)

        modificacion = vencimiento if estado != "Pendiente" else fecha_desembolso
        cuotas.append({
            "numero_credito": numero_credito,
            "numero_cuota": numero_cuota,
            "fecha_vencimiento": vencimiento.isoformat(),
            "monto_cuota": monto_cuota,
            "monto_capital": capital,
            "monto_interes": interes,
            "estado_cuota": estado,
            "fecha_creacion": f"{fecha_desembolso} 00:00:00",
            "fecha_modificacion": f"{modificacion} 00:00:00",
        })

        if estado == "Pagada":
            atraso = 0 if random.random() < 0.75 else random.randint(1, 20)
            fecha_pago = datetime.combine(vencimiento, datetime.min.time()) + timedelta(days=atraso)
            pagos.append({
                "numero_credito": numero_credito,
                "numero_cuota": numero_cuota,
                "fecha_pago": fecha_pago,
                "monto_pagado": monto_cuota,
                "medio_pago": random.choices(MEDIOS_PAGO, weights=PESO_MEDIOS_PAGO)[0],
                "fecha_creacion": fecha_pago,
                "fecha_modificacion": fecha_pago,
            })

    if vencidas:
        primera = min(vencidas)
        dias_mora = max((HOY - primera).days, 1)
        for _ in range(random.randint(1, 4)):
            fecha_gestion = datetime.combine(fecha_aleatoria(primera, HOY), datetime.min.time())
            gestiones.append({
                "numero_credito": numero_credito,
                "fecha_gestion": fecha_gestion,
                "tipo_gestion": random.choices(TIPOS_GESTION, weights=PESO_TIPOS)[0],
                "resultado": random.choices(RESULTADOS_GESTION, weights=PESO_RESULTADOS)[0],
                "dias_mora_al_momento": dias_mora,
                "gestor": random.choice(NOMBRES_GESTOR),
                "fecha_creacion": fecha_gestion,
                "fecha_modificacion": fecha_gestion,
            })
    return cuotas, pagos, gestiones


def generar_full(n_creditos: int, seed: int) -> dict[str, list[dict]]:
    random.seed(seed)
    datos = {tabla: [] for tabla in ORDEN_CARGA}
    for numero_credito in range(1, n_creditos + 1):
        cuotas, pagos, gestiones = generar_credito_historico(numero_credito)
        datos["cuotas"].extend(cuotas)
        datos["pagos"].extend(pagos)
        datos["gestiones_cobranza"].extend(gestiones)
    return datos


def generar_incremental(numero_inicial: int, n_nuevos: int, seed: int) -> dict[str, list[dict]]:
    random.seed(seed)
    ahora = datetime.utcnow()
    cuotas: list[dict] = []
    for offset in range(n_nuevos):
        numero_credito = numero_inicial + offset
        plazo = random.randint(PLAZO_MIN, PLAZO_MAX)
        monto_cuota = round(random.uniform(150, 2500), 2)
        for numero_cuota in range(1, plazo + 1):
            interes = round(monto_cuota * 0.18, 2)
            cuotas.append({
                "numero_credito": numero_credito,
                "numero_cuota": numero_cuota,
                "fecha_vencimiento": sumar_meses(HOY, numero_cuota).isoformat(),
                "monto_cuota": monto_cuota,
                "monto_capital": round(monto_cuota - interes, 2),
                "monto_interes": interes,
                "estado_cuota": "Pendiente",
                "fecha_creacion": ahora,
                "fecha_modificacion": ahora,
            })
    return {"cuotas": cuotas, "pagos": [], "gestiones_cobranza": []}


def cargar_driver():
    try:
        import pyodbc
    except ImportError as error:
        raise SystemExit("ERROR: falta pyodbc y el ODBC Driver 18 for SQL Server.") from error
    return pyodbc


def max_credito(conn, esquema: str) -> int:
    cursor = conn.cursor()
    cursor.execute(f"SELECT ISNULL(MAX(numero_credito), 0) FROM {esquema}.cuotas")
    value = int(cursor.fetchone()[0])
    cursor.close()
    return value


def actualizar_cuotas_existentes(dsn: str, esquema: str, n_pagar: int, n_vencer: int) -> tuple[int, int]:
    pyodbc = cargar_driver()
    conn = pyodbc.connect(dsn, autocommit=False)
    cursor = conn.cursor()
    ahora = datetime.utcnow()
    cursor.execute(
        f"SELECT TOP {n_pagar + n_vencer} id_cuota, numero_credito, numero_cuota, monto_cuota "
        f"FROM {esquema}.cuotas WHERE estado_cuota = 'Pendiente' ORDER BY NEWID()"
    )
    filas = cursor.fetchall()
    a_pagar, a_vencer = filas[:n_pagar], filas[n_pagar:n_pagar + n_vencer]
    for id_cuota, numero_credito, numero_cuota, monto_cuota in a_pagar:
        cursor.execute(
            f"UPDATE {esquema}.cuotas SET estado_cuota = 'Pagada', fecha_modificacion = ? WHERE id_cuota = ?",
            ahora, id_cuota,
        )
        cursor.execute(
            f"INSERT INTO {esquema}.pagos "
            "(numero_credito, numero_cuota, fecha_pago, monto_pagado, medio_pago, fecha_creacion, fecha_modificacion) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            numero_credito, numero_cuota, ahora, monto_cuota,
            random.choices(MEDIOS_PAGO, weights=PESO_MEDIOS_PAGO)[0], ahora, ahora,
        )
    for id_cuota, numero_credito, _numero_cuota, _monto_cuota in a_vencer:
        cursor.execute(
            f"UPDATE {esquema}.cuotas SET estado_cuota = 'Vencida', fecha_modificacion = ? WHERE id_cuota = ?",
            ahora, id_cuota,
        )
        cursor.execute(
            f"INSERT INTO {esquema}.gestiones_cobranza "
            "(numero_credito, fecha_gestion, tipo_gestion, resultado, dias_mora_al_momento, gestor, fecha_creacion, fecha_modificacion) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            numero_credito, ahora, random.choices(TIPOS_GESTION, weights=PESO_TIPOS)[0],
            random.choices(RESULTADOS_GESTION, weights=PESO_RESULTADOS)[0],
            random.randint(1, 10), random.choice(NOMBRES_GESTOR), ahora, ahora,
        )
    conn.commit()
    cursor.close()
    conn.close()
    return len(a_pagar), len(a_vencer)


def escribir_csv(datos: dict[str, list[dict]], salida: Path) -> None:
    salida.mkdir(parents=True, exist_ok=True)
    for tabla in ORDEN_CARGA:
        filas = datos[tabla]
        if not filas:
            continue
        with (salida / f"{tabla}.csv").open("w", newline="", encoding="utf-8") as archivo:
            writer = csv.DictWriter(archivo, fieldnames=list(filas[0]))
            writer.writeheader()
            writer.writerows(filas)
        print(f"  {tabla:<20} {len(filas):>9,} filas -> {salida / (tabla + '.csv')}")


def escribir_sql(datos: dict[str, list[dict]], dsn: str, esquema: str, truncar: bool) -> None:
    pyodbc = cargar_driver()
    conn = pyodbc.connect(dsn, autocommit=False)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    if truncar:
        for tabla in reversed(ORDEN_CARGA):
            cursor.execute(f"DELETE FROM {esquema}.{tabla}")
        conn.commit()
    for tabla in ORDEN_CARGA:
        filas = datos[tabla]
        if not filas:
            continue
        columnas = list(filas[0])
        sql = f"INSERT INTO {esquema}.{tabla} ({', '.join(columnas)}) VALUES ({', '.join('?' * len(columnas))})"
        valores = [tuple(fila[columna] for columna in columnas) for fila in filas]
        for inicio in range(0, len(valores), 5000):
            cursor.executemany(sql, valores[inicio:inicio + 5000])
        conn.commit()
        print(f"  {tabla:<20} {len(filas):>9,} filas insertadas")
    cursor.close()
    conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera datos locales para el pipeline ADF de cobranzas.")
    parser.add_argument("--destino", choices=("csv", "local"), default="csv")
    parser.add_argument("--modo", choices=("full", "incremental"), default="full")
    parser.add_argument("--salida", type=Path, default=Path("datos_cobranzas"))
    parser.add_argument("--dsn", default=os.getenv("COBRANZAS_DSN"))
    parser.add_argument("--n-creditos", type=int, default=N_CREDITOS_DEFAULT)
    parser.add_argument("--nuevos-creditos", type=int, default=25)
    parser.add_argument("--cuotas-pagar", type=int, default=30)
    parser.add_argument("--cuotas-vencer", type=int, default=15)
    parser.add_argument("--truncar", action="store_true")
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()

    if args.destino == "local" and not args.dsn:
        raise SystemExit("ERROR: falta --dsn o COBRANZAS_DSN para --destino local.")
    esquema = esquema_de(args.destino)

    if args.modo == "full":
        datos = generar_full(args.n_creditos, args.seed)
    else:
        if args.destino != "local":
            raise SystemExit("ERROR: --modo incremental requiere --destino local.")
        pyodbc = cargar_driver()
        conn = pyodbc.connect(args.dsn, autocommit=True)
        inicial = max_credito(conn, esquema) + 1
        conn.close()
        datos = generar_incremental(inicial, args.nuevos_creditos, args.seed)

    print(f"Modo={args.modo}; destino={args.destino}; schema={esquema}")
    if args.destino == "csv":
        escribir_csv(datos, args.salida)
    else:
        escribir_sql(datos, args.dsn, esquema, args.truncar and args.modo == "full")
        if args.modo == "incremental":
            pagadas, vencidas = actualizar_cuotas_existentes(
                args.dsn, esquema, args.cuotas_pagar, args.cuotas_vencer
            )
            print(f"cuotas pagadas={pagadas}; cuotas vencidas={vencidas}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
