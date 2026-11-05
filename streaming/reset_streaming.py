"""Deja la parte B en cero antes de una corrida: vacia las 4 tablas y borra stream/raw/ en S3.

Uso: python reset_streaming.py --bucket secop-trabajo2-equipo8-usw2
"""
import argparse

import boto3

TABLES = {"secop-windows": ["modalidad", "window_start"], "secop-seen-events": ["event_id"],
          "secop-invalid-events": ["sequence_number"], "secop-counters": ["metric"]}

ap = argparse.ArgumentParser()
ap.add_argument("--bucket", default="secop-trabajo2-equipo8-usw2")
args = ap.parse_args()
ddb = boto3.resource("dynamodb")
for name, keys in TABLES.items():
    t, n, kw = ddb.Table(name), 0, {}
    with t.batch_writer() as bw:
        while True:
            page = t.scan(ProjectionExpression=", ".join(f"#{i}" for i in range(len(keys))),
                          ExpressionAttributeNames={f"#{i}": k for i, k in enumerate(keys)}, **kw)
            for item in page["Items"]:
                bw.delete_item(Key=item)
                n += 1
            if "LastEvaluatedKey" not in page:
                break
            kw = {"ExclusiveStartKey": page["LastEvaluatedKey"]}
    print(f"{name}: {n} items borrados")
b = boto3.resource("s3").Bucket(args.bucket)
print("stream/raw:", len(b.objects.filter(Prefix="stream/raw/").delete()), "lotes de borrado")
