"""Descarga procesos SECOP II (p6dx-8zbt) por año y proponentes (hgi6-6wh3) en CSV.

Paginación por llave (:id) con la API Socrata de datos.gov.co.
Uso: python download_secop.py procesos 2024   |   python download_secop.py proponentes
"""
import csv
import io
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASE = "https://www.datos.gov.co/resource/{}.csv"
PAGE = 50000
PROCESOS_COLS = [
    "id_del_proceso", "entidad", "nit_entidad", "departamento_entidad", "ciudad_entidad", "ordenentidad",
    "fecha_de_publicacion_del", "precio_base", "modalidad_de_contratacion", "duracion", "unidad_de_duracion",
    "estado_del_procedimiento", "adjudicado", "valor_total_adjudicacion", "codigo_principal_de_categoria",
    "tipo_de_contrato", "fase"
]
PROPONENTES_COLS = ["id_procedimiento", "fecha_publicaci_n", "codigo_entidad", "codigo_proveedor"]
OUT = Path(__file__).resolve().parent.parent / "data" / "raw"


def fetch(dataset, cols, where, last_id):
    parts = ([f":id > '{last_id}'"] if last_id else []) + ([where] if where else [])
    params = {"$select": ":id," + ",".join(cols), "$order": ":id", "$limit": PAGE}
    if parts:
        params["$where"] = " AND ".join(parts)
    url = BASE.format(dataset) + "?" + urllib.parse.urlencode(params)
    for attempt in range(20):
        try:
            with urllib.request.urlopen(url, timeout=300) as r:
                return list(csv.reader(io.StringIO(r.read().decode("utf-8"))))
        except Exception as e:  # noqa: BLE001
            print(f"retry {attempt}: {e}", flush=True)
            time.sleep(min(60, 10 * (attempt + 1)))
    raise RuntimeError("too many retries")


def run(dataset, cols, where, out_file):
    out_file.parent.mkdir(parents=True, exist_ok=True)
    last_id, total = "", 0
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        while True:
            rows = fetch(dataset, cols, where, last_id)
            body = rows[1:]
            if not body:
                break
            w.writerows(r[1:] for r in body)
            last_id = body[-1][0]
            total += len(body)
            print(f"{out_file.name}: {total}", flush=True)
    print(f"DONE {out_file.name} {total}", flush=True)


if __name__ == "__main__":
    if sys.argv[1] == "procesos":
        y = sys.argv[2]
        where = f"fecha_de_publicacion_del between '{y}-01-01T00:00:00' and '{y}-12-31T23:59:59'"
        run("p6dx-8zbt", PROCESOS_COLS, where, OUT / "procesos" / f"procesos_{y}.csv")
    else:
        run("hgi6-6wh3", PROPONENTES_COLS, "", OUT / "proponentes" / "proponentes.csv")
