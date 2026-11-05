"""Trabajo de Glue inicial: limpia procesos SECOP II, los une con proponentes y escribe Parquet.

Lee CSV desde S3 (raw), descarta y corrige registros no validos, une cada proceso con el
numero de empresas que presentaron oferta (join por id del proceso) y escribe en S3 (curated)
en Parquet particionado por anio_publicacion.

Parametros del trabajo: --BUCKET (nombre del bucket, sin s3://).
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

bucket = args["BUCKET"]
src_procesos = f"s3://{bucket}/raw/procesos/"
src_proponentes = f"s3://{bucket}/raw/proponentes/"
target = f"s3://{bucket}/curated/procesos/"

csv_opts = {"header": "true", "inferSchema": "false", "escape": '"', "multiLine": "false"}
MAX_VALOR_COP = 5e12  # 5 billones de COP, cerca del 1 % del presupuesto general de la nacion
NO_DEFINIDO = ["", "NO DEFINIDO", "NO DEFINIDA", "NO APLICA", "N/A", "NULL"]


def clean_text(col):
    """Quita espacios repetidos y convierte a nulo los valores vacios o 'No definido'."""
    c = F.regexp_replace(F.trim(F.col(col)), r"\s+", " ")
    return F.when(F.upper(c).isin(NO_DEFINIDO), None).otherwise(c)


# ---------- 1. Lectura ----------
procesos_raw = spark.read.options(**csv_opts).csv(src_procesos)
proponentes_raw = spark.read.options(**csv_opts).csv(src_proponentes)
n_procesos_raw = procesos_raw.count()
n_proponentes_raw = proponentes_raw.count()

# ---------- 2. Limpieza de procesos ----------
text_cols = [
    "id_del_proceso", "entidad", "nit_entidad", "departamento_entidad", "ciudad_entidad", "ordenentidad",
    "modalidad_de_contratacion", "unidad_de_duracion", "estado_del_procedimiento", "adjudicado",
    "codigo_principal_de_categoria", "tipo_de_contrato", "fase"
]
procesos = procesos_raw
for c in text_cols:
    procesos = procesos.withColumn(c, clean_text(c))

procesos = (
    procesos
    .withColumn("fecha_publicacion", F.to_date(F.substring("fecha_de_publicacion_del", 1, 10)))
    .withColumn("precio_base", F.col("precio_base").cast("double"))
    .withColumn("valor_total_adjudicacion", F.col("valor_total_adjudicacion").cast("double"))
    .withColumn("duracion", F.col("duracion").cast("int"))
    # Correcciones: valores monetarios negativos, en cero o mayores que MAX_VALOR_COP pasan a nulo
    # (no son un precio real; los mayores son errores de digitacion).
    .withColumn("precio_base_original", F.col("precio_base"))
    .withColumn("precio_base", F.when(F.col("precio_base").between(1, MAX_VALOR_COP), F.col("precio_base")))
    .withColumn("valor_total_adjudicacion",
                F.when(F.col("valor_total_adjudicacion").between(1, MAX_VALOR_COP), F.col("valor_total_adjudicacion")))
    .withColumn("duracion", F.when(F.col("duracion") >= 0, F.col("duracion")))
    # Correccion: departamento en mayusculas y sin tildes para que agrupe bien.
    .withColumn("departamento_entidad",
                F.upper(F.translate("departamento_entidad", "áéíóúÁÉÍÓÚñÑ", "aeiouAEIOUnN")))
    .withColumn("adjudicado", F.when(F.upper("adjudicado") == "SI", True)
                .when(F.upper("adjudicado") == "NO", False))
    # Segmento UNSPSC: los dos primeros digitos del codigo de categoria (V1.80111500 -> 80).
    .withColumn("segmento_unspsc", F.regexp_extract("codigo_principal_de_categoria", r"(\d{2})\d{6}", 1))
    .withColumn("segmento_unspsc", F.when(F.col("segmento_unspsc") != "", F.col("segmento_unspsc")))
)

# Descartes: sin identificador, sin fecha valida o fuera del rango descargado.
procesos_validos = procesos.filter(
    F.col("id_del_proceso").isNotNull()
    & F.col("fecha_publicacion").isNotNull()
    & F.year("fecha_publicacion").between(2022, 2026)
).persist(StorageLevel.MEMORY_AND_DISK)
n_invalidos = n_procesos_raw - procesos_validos.count()
n_precio_corregido = procesos_validos.filter(
    F.col("precio_base_original").isNotNull() & F.col("precio_base").isNull()).count()

# Duplicados: un mismo proceso puede aparecer varias veces; se conserva la fila mas reciente.
w = Window.partitionBy("id_del_proceso").orderBy(F.col("fecha_publicacion").desc(),
                                                 F.col("precio_base").desc_nulls_last())
procesos_unicos = (procesos_validos.withColumn("_rn", F.row_number().over(w))
                   .filter("_rn = 1").drop("_rn", "fecha_de_publicacion_del", "precio_base_original")
                   .persist(StorageLevel.MEMORY_AND_DISK))
n_procesos_ok = procesos_unicos.count()

# ---------- 3. Limpieza de proponentes ----------
proponentes = (
    proponentes_raw
    .withColumn("id_procedimiento", clean_text("id_procedimiento"))
    .withColumn("codigo_proveedor", clean_text("codigo_proveedor"))
    .filter(F.col("id_procedimiento").isNotNull() & F.col("codigo_proveedor").isNotNull())
    .dropDuplicates(["id_procedimiento", "codigo_proveedor"])
)
ofertas = (proponentes.groupBy(F.col("id_procedimiento").alias("id_del_proceso"))
           .agg(F.countDistinct("codigo_proveedor").alias("num_proponentes")))

# ---------- 4. Union (join) por id del proceso ----------
resultado = (
    procesos_unicos.join(ofertas, on="id_del_proceso", how="left")
    .withColumn("num_proponentes", F.coalesce(F.col("num_proponentes"), F.lit(0)).cast("int"))
    .withColumn("tiene_competencia", F.col("num_proponentes") >= 2)
    .withColumn("anio_publicacion", F.year("fecha_publicacion"))
    .withColumn("mes_publicacion", F.month("fecha_publicacion"))
)
n_con_ofertas = resultado.filter("num_proponentes > 0").count()

# ---------- 5. Escritura en Parquet particionado por anio ----------
(resultado.repartition("anio_publicacion").write.mode("overwrite")
 .partitionBy("anio_publicacion").parquet(target))

print(f"[conteos] procesos_leidos={n_procesos_raw} proponentes_leidos={n_proponentes_raw} "
      f"procesos_descartados_invalidos={n_invalidos} "
      f"procesos_duplicados_descartados={n_procesos_raw - n_invalidos - n_procesos_ok} "
      f"precios_corregidos_a_nulo={n_precio_corregido} "
      f"procesos_escritos={n_procesos_ok} procesos_con_ofertas={n_con_ofertas}")
job.commit()
