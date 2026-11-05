"""Rama 1: competencia por departamento y anio.

Pregunta de analitica: en que departamentos los procesos con ofertas reciben menos
competencia (mayor porcentaje de procesos con un solo oferente)? Ahi una MiPyme tiene
mas oportunidad de entrar y el Estado mas necesidad de atraer oferentes.

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

procesos = spark.read.parquet(f"s3://{bucket}/curated/procesos/")
con_ofertas = procesos.filter((F.col("num_proponentes") > 0) & F.col("departamento_entidad").isNotNull())

resumen = (
    con_ofertas.groupBy("departamento_entidad", "anio_publicacion")
    .agg(
        F.count("*").alias("procesos_con_ofertas"),
        F.sum(F.when(F.col("num_proponentes") == 1, 1).otherwise(0)).alias("procesos_un_oferente"),
        F.round(F.avg("num_proponentes"), 2).alias("promedio_oferentes"),
        F.round(F.sum("precio_base") / 1e6, 1).alias("presupuesto_millones_cop"),
    )
    .withColumn("pct_un_oferente",
                F.round(100 * F.col("procesos_un_oferente") / F.col("procesos_con_ofertas"), 1))
)

resumen.coalesce(1).write.mode("overwrite").parquet(f"s3://{bucket}/analytics/competencia_departamento/")
print(f"[conteos] filas_agrupacion={resumen.count()}")
job.commit()
