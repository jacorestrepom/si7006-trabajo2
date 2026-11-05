"""Muestra que los conteos cuadran: publicados = unicos procesados + repetidos + invalidos.

Compara tres fuentes: producer_counts.json (lo que publico el productor), la tabla
secop-counters (lo que conto la Lambda de ventanas) y los eventos crudos en S3 (segundo consumidor).

Uso: python verify_counts.py --bucket secop-trabajo2-equipo8-usw2
"""
import argparse
import json

import boto3

ap = argparse.ArgumentParser()
ap.add_argument("--bucket", default="secop-trabajo2-equipo8-usw2")
ap.add_argument("--producer-file", default="producer_counts.json")
args = ap.parse_args()

prod = json.load(open(args.producer_file, encoding="utf-8"))
ddb = boto3.resource("dynamodb").Table("secop-counters")
c = {i["metric"]: int(i["total"]) for i in ddb.scan()["Items"]}
s3 = boto3.client("s3")
crudos = 0
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=args.bucket, Prefix="stream/raw/"):
    for o in page.get("Contents", []):
        body = s3.get_object(Bucket=args.bucket, Key=o["Key"])["Body"].read().decode("utf-8")
        crudos += len([l for l in body.split("\n") if l.strip()])
inv_tabla = boto3.client("dynamodb").scan(TableName="secop-invalid-events", Select="COUNT")["Count"]

u, r, i = c.get("unicos_procesados", 0), c.get("repetidos_descartados", 0), c.get("invalidos_descartados", 0)
print(f"Productor  -> publicados={prod['publicados']} (unicos={prod['unicos_validos']}, "
      f"repetidos={prod['repetidos']}, invalidos={prod['invalidos']})")
print(f"Lambda     -> unicos_procesados={u}  repetidos_descartados={r}  invalidos_descartados={i}  "
      f"recibidos={c.get('recibidos', 0)}")
print(f"S3 crudos  -> {crudos} eventos   |   secop-invalid-events -> {inv_tabla} filas")
print(f"\n{prod['publicados']} publicados = {u} + {r} + {i} = {u + r + i}  ->  "
      f"{'CUADRA' if prod['publicados'] == u + r + i == crudos else 'NO CUADRA'}")
