"""Lambda consumidora 2: guarda en S3 todos los eventos tal como llegan del stream.

Escribe un archivo JSON Lines por lote en s3://<BUCKET>/stream/raw/dt=AAAA-MM-DD/.
Cada linea tiene el payload original sin modificar y los metadatos de Kinesis.
Variable de entorno: BUCKET.
"""
import base64
import json
import os
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")


def lambda_handler(event, context):
    lineas = []
    for rec in event["Records"]:
        k = rec["kinesis"]
        lineas.append(json.dumps({
            "sequence_number": k["sequenceNumber"],
            "partition_key": k["partitionKey"],
            "arrival_time": k["approximateArrivalTimestamp"],
            "payload": base64.b64decode(k["data"]).decode("utf-8", errors="replace"),
        }, ensure_ascii=False))
    ahora = datetime.now(timezone.utc)
    first = event["Records"][0]["kinesis"]["sequenceNumber"]
    key = f"stream/raw/dt={ahora:%Y-%m-%d}/{ahora:%H%M%S}-{first}.jsonl"
    s3.put_object(Bucket=os.environ.get("BUCKET", "secop-trabajo2-equipo8-usw2"), Key=key, Body="\n".join(lineas).encode("utf-8"))
    return {"guardados": len(lineas), "key": key}
