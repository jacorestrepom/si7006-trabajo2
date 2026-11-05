"""Consulta la tabla secop-windows para responder una pregunta del proyecto.

Pregunta: durante la corrida, que modalidad de contratacion publico mas procesos por minuto y
cual tuvo el mayor valor promedio? Sirve para saber en que modalidades aparecen mas
oportunidades nuevas (las que el recomendador debe revisar con mas frecuencia).

Uso: python query_windows.py
"""
from collections import defaultdict

import boto3
from boto3.dynamodb.conditions import Key

table = boto3.resource("dynamodb").Table("secop-windows")
items, kw = [], {}
while True:
    page = table.scan(**kw)
    items += page["Items"]
    if "LastEvaluatedKey" not in page:
        break
    kw = {"ExclusiveStartKey": page["LastEvaluatedKey"]}

print(f"{len(items)} ventanas (modalidad x minuto)\n")
print(f"{'ventana':17} {'modalidad':40} {'conteo':>6} {'promedio (M COP)':>17}")
for it in sorted(items, key=lambda x: (x["window_start"], x["modalidad"])):
    print(f"{it['window_start']:17} {it['modalidad'][:40]:40} {int(it['event_count']):6} {float(it['precio_promedio']):17,.2f}")

tot = defaultdict(lambda: [0, 0.0, 0])
for it in items:
    t = tot[it["modalidad"]]
    t[0] += int(it["event_count"])
    t[1] += float(it["precio_sum"])
    t[2] += 1
print("\nResumen por modalidad (todas las ventanas):")
print(f"{'modalidad':40} {'eventos':>7} {'eventos/min':>11} {'promedio (M COP)':>17}")
for m, (n, s, w) in sorted(tot.items(), key=lambda kv: -kv[1][0]):
    print(f"{m[:40]:40} {n:7} {n / w:11.1f} {s / n:17,.2f}")

ejemplo = "Licitación pública"
r = table.query(KeyConditionExpression=Key("modalidad").eq(ejemplo))
print(f"\nQuery por llave de particion modalidad = '{ejemplo}': {r['Count']} ventanas")
