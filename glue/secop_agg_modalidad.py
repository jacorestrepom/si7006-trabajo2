"""Rama 2: presupuesto por tipo de contrato y modalidad de contratacion.

Pregunta de analitica: que tipos de contrato y modalidades concentran el mayor presupuesto
publicado, y que parte de ese presupuesto se contrata de forma directa (sin competencia)?
Orienta en que mercados el recomendador encuentra mas procesos abiertos a ofertas.

Parametros del trabajo: --BUCKET.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
glue_context = GlueContext(SparkContext())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
bucket = args["BUCKET"]

procesos = spark.read.parquet(f"s3://{bucket}/curated/procesos/").filter(
    F.col("tipo_de_contrato").isNotNull() & F.col("modalidad_de_contratacion").isNotNull())

resumen = (
    procesos.groupBy("tipo_de_contrato", "modalidad_de_contratacion")
    .agg(
        F.count("*").alias("num_procesos"),
        F.round(F.sum("precio_base") / 1e9, 2).alias("presupuesto_miles_millones_cop"),
        F.round(F.expr("percentile_approx(precio_base, 0.5)") / 1e6, 2).alias("precio_mediano_millones_cop"),
        F.round(100 * F.avg(F.col("adjudicado").cast("int")), 1).alias("pct_adjudicado"),
        F.round(F.avg("num_proponentes"), 2).alias("promedio_oferentes"),
    )
    .withColumn("es_contratacion_directa",
                F.lower(F.col("modalidad_de_contratacion")).contains("directa"))
)

resumen.coalesce(1).write.mode("overwrite").parquet(f"s3://{bucket}/analytics/presupuesto_modalidad/")
print(f"[conteos] filas_agrupacion={resumen.count()}")
job.commit()
