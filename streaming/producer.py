"""Productor: publica en Kinesis Data Streams procesos SECOP simulados durante al menos 5 minutos.

Cada evento es un proceso de contratacion recien publicado:
  {"event_id", "id_proceso", "modalidad", "departamento", "precio_base_millones", "event_time"}
La categoria es "modalidad" y el campo numerico es "precio_base_millones".

A proposito:
  - repite eventos ya enviados (mismo event_id y mismo contenido),
  - envia eventos invalidos: sin modalidad, sin event_id, precio fuera de rango (0, 50000] millones
    o JSON mal formado.
Al terminar escribe los conteos en producer_counts.json.

Uso: python producer.py --stream secop-procesos --minutes 6 --rate 5
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import boto3

MODALIDADES = {
    "Contratación directa": (0.62, 60),
    "Contratación régimen especial": (0.15, 250),
    "Mínima cuantía": (0.12, 15),
    "Selección abreviada de menor cuantía": (0.05, 400),
    "Licitación pública": (0.03, 3000),
    "Concurso de méritos abierto": (0.03, 900),
}
DEPARTAMENTOS = ["DISTRITO CAPITAL DE BOGOTA", "ANTIOQUIA", "VALLE DEL CAUCA", "SANTANDER", "CUNDINAMARCA",
                 "ATLANTICO", "BOLIVAR", "NARINO", "TOLIMA", "BOYACA"]
PRECIO_MAX = 50000


def nuevo_evento():
    nombres = list(MODALIDADES)
    modalidad = random.choices(nombres, weights=[MODALIDADES[m][0] for m in nombres])[0]
    precio = round(random.lognormvariate(0, 1) * MODALIDADES[modalidad][1], 2)
    return {
        "event_id": str(uuid.uuid4()),
        "id_proceso": f"CO1.REQ.{random.randint(10_000_000, 99_999_999)}",
        "modalidad": modalidad,
        "departamento": random.choice(DEPARTAMENTOS),
        "precio_base_millones": min(precio, PRECIO_MAX),
        "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def evento_invalido():
    e = nuevo_evento()
    tipo = random.choice(["sin_modalidad", "sin_event_id", "precio_negativo", "precio_alto", "json_malo"])
    if tipo == "sin_modalidad":
        del e["modalidad"]
    elif tipo == "sin_event_id":
        del e["event_id"]
    elif tipo == "precio_negativo":
        e["precio_base_millones"] = -round(random.uniform(1, 500), 2)
    elif tipo == "precio_alto":
        e["precio_base_millones"] = round(random.uniform(PRECIO_MAX + 1, 10 * PRECIO_MAX), 2)
    else:
        return tipo, (json.dumps(e)[:-7]).encode()
    return tipo, json.dumps(e, ensure_ascii=False).encode()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", default="secop-procesos")
    ap.add_argument("--minutes", type=float, default=6)
    ap.add_argument("--rate", type=float, default=5, help="eventos por segundo")
    ap.add_argument("--dup-prob", type=float, default=0.07)
    ap.add_argument("--invalid-prob", type=float, default=0.06)
    args = ap.parse_args()

    kinesis = boto3.client("kinesis")
    enviados = []
    counts = {"publicados": 0, "unicos_validos": 0, "repetidos": 0, "invalidos": 0, "invalidos_por_tipo": {}}
    fin = time.time() + args.minutes * 60
    while time.time() < fin:
        inicio = time.time()
        lote = []
        for _ in range(max(1, round(args.rate))):
            r = random.random()
            if r < args.dup_prob and enviados:
                data, key = random.choice(enviados[-200:])
                kind = "repetidos"
            elif r < args.dup_prob + args.invalid_prob:
                tipo, data = evento_invalido()
                key, kind = str(uuid.uuid4()), "invalidos"
                counts["invalidos_por_tipo"][tipo] = counts["invalidos_por_tipo"].get(tipo, 0) + 1
            else:
                e = nuevo_evento()
                data, key, kind = json.dumps(e, ensure_ascii=False).encode(), e["event_id"], "unicos_validos"
                enviados.append((data, key))
            lote.append(({"Data": data, "PartitionKey": key}, kind))
        pendientes = lote
        while pendientes:
            resp = kinesis.put_records(StreamName=args.stream, Records=[r for r, _ in pendientes])
            fallidos = []
            for (rec, kind), res in zip(pendientes, resp["Records"]):
                if "ErrorCode" in res:
                    fallidos.append((rec, kind))
                else:
                    counts["publicados"] += 1
                    counts[kind] += 1
            pendientes = fallidos
        print(json.dumps(counts, ensure_ascii=False), flush=True)
        time.sleep(max(0, 1 - (time.time() - inicio)))

    counts["fin"] = datetime.now(timezone.utc).isoformat()
    with open("producer_counts.json", "w", encoding="utf-8") as f:
        json.dump(counts, f, ensure_ascii=False, indent=2)
    print("FINAL", json.dumps(counts, ensure_ascii=False))


if __name__ == "__main__":
    main()
