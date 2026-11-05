"""Lambda consumidora 1: valida, quita repetidos y agrega por ventanas de un minuto.

Disparador: el stream de Kinesis secop-procesos.
- Invalidos -> tabla DynamoDB secop-invalid-events, con el motivo del descarte.
- Repetidos -> se reconocen por event_id con una escritura condicional en secop-seen-events;
  si el event_id ya existe, el evento no se cuenta otra vez.
- Unicos validos -> tabla secop-windows: por (modalidad, inicio de la ventana de 1 minuto
  segun event_time) se acumulan conteo y suma del precio, y se guarda el promedio.
- secop-counters guarda los totales de unicos, repetidos e invalidos.
Variables de entorno: WINDOWS_TABLE, SEEN_TABLE, INVALID_TABLE, COUNTERS_TABLE.
"""
import base64
import json
import os
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

ddb = boto3.resource("dynamodb")
windows = ddb.Table(os.environ.get("WINDOWS_TABLE", "secop-windows"))
seen = ddb.Table(os.environ.get("SEEN_TABLE", "secop-seen-events"))
invalid = ddb.Table(os.environ.get("INVALID_TABLE", "secop-invalid-events"))
counters = ddb.Table(os.environ.get("COUNTERS_TABLE", "secop-counters"))
PRECIO_MAX = 50000


def validar(raw):
    """Devuelve (evento, None) si es valido o (None, motivo) si no."""
    try:
        e = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, "JSON mal formado"
    if not isinstance(e, dict):
        return None, "JSON no es un objeto"
    for campo in ("event_id", "modalidad", "precio_base_millones", "event_time"):
        if e.get(campo) in (None, ""):
            return None, f"falta el campo {campo}"
    try:
        precio = float(e["precio_base_millones"])
    except (TypeError, ValueError):
        return None, "precio_base_millones no es numerico"
    if not 0 < precio <= PRECIO_MAX:
        return None, f"precio_base_millones={precio} fuera del rango (0, {PRECIO_MAX}]"
    try:
        datetime.fromisoformat(e["event_time"])
    except ValueError:
        return None, "event_time no es una fecha valida"
    return e, None


def lambda_handler(event, context):
    agregados = defaultdict(lambda: [0, Decimal("0")])
    n_unicos = n_rep = n_inv = 0
    for rec in event["Records"]:
        k = rec["kinesis"]
        raw = base64.b64decode(k["data"]).decode("utf-8", errors="replace")
        e, motivo = validar(raw)
        if motivo:
            invalid.put_item(Item={"sequence_number": k["sequenceNumber"], "motivo": motivo, "payload": raw,
                                   "arrival_time": str(k["approximateArrivalTimestamp"])})
            n_inv += 1
            continue
        try:
            seen.put_item(Item={"event_id": e["event_id"], "first_sequence_number": k["sequenceNumber"]},
                          ConditionExpression="attribute_not_exists(event_id)")
        except ClientError as err:
            if err.response["Error"]["Code"] == "ConditionalCheckFailedException":
                n_rep += 1
                continue
            raise
        ventana = datetime.fromisoformat(e["event_time"]).strftime("%Y-%m-%dT%H:%M")
        a = agregados[(e["modalidad"], ventana)]
        a[0] += 1
        a[1] += Decimal(str(e["precio_base_millones"]))
        n_unicos += 1

    for (modalidad, ventana), (n, suma) in agregados.items():
        r = windows.update_item(
            Key={"modalidad": modalidad, "window_start": ventana},
            UpdateExpression="ADD event_count :n, precio_sum :s",
            ExpressionAttributeValues={":n": n, ":s": suma}, ReturnValues="ALL_NEW")["Attributes"]
        windows.update_item(
            Key={"modalidad": modalidad, "window_start": ventana},
            UpdateExpression="SET precio_promedio = :p, window_end = :e",
            ExpressionAttributeValues={":p": round(r["precio_sum"] / r["event_count"], 2),
                                       ":e": ventana + ":59"})

    for metric, n in (("unicos_procesados", n_unicos), ("repetidos_descartados", n_rep),
                      ("invalidos_descartados", n_inv), ("recibidos", len(event["Records"]))):
        if n:
            counters.update_item(Key={"metric": metric}, UpdateExpression="ADD #t :n",
                                 ExpressionAttributeNames={"#t": "total"}, ExpressionAttributeValues={":n": n})
    return {"unicos": n_unicos, "repetidos": n_rep, "invalidos": n_inv}
