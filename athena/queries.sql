-- Parte A: consultas de las dos agrupaciones con Amazon Athena (region us-west-2).
-- Ejecutar una sentencia a la vez en el editor de consultas de Athena.

-- 1. Base de datos
CREATE DATABASE IF NOT EXISTS secop_trabajo2;

-- 2. Tabla de la rama 1 (competencia por departamento y anio)
CREATE EXTERNAL TABLE IF NOT EXISTS secop_trabajo2.competencia_departamento (
  departamento_entidad string,
  anio_publicacion int,
  procesos_con_ofertas bigint,
  procesos_un_oferente bigint,
  promedio_oferentes double,
  presupuesto_millones_cop double,
  pct_un_oferente double
)
STORED AS PARQUET
LOCATION 's3://secop-trabajo2-equipo8-usw2/analytics/competencia_departamento/';

-- 3. Tabla de la rama 2 (presupuesto por tipo de contrato y modalidad)
CREATE EXTERNAL TABLE IF NOT EXISTS secop_trabajo2.presupuesto_modalidad (
  tipo_de_contrato string,
  modalidad_de_contratacion string,
  num_procesos bigint,
  presupuesto_miles_millones_cop double,
  precio_mediano_millones_cop double,
  pct_adjudicado double,
  promedio_oferentes double,
  es_contratacion_directa boolean
)
STORED AS PARQUET
LOCATION 's3://secop-trabajo2-equipo8-usw2/analytics/presupuesto_modalidad/';

-- 4. Pregunta rama 1: en que departamentos los procesos con ofertas tienen menos competencia?
--    (departamentos con al menos 1.000 procesos con ofertas entre 2022 y 2026)
SELECT departamento_entidad,
       SUM(procesos_con_ofertas) AS procesos_con_ofertas,
       ROUND(100.0 * SUM(procesos_un_oferente) / SUM(procesos_con_ofertas), 1) AS pct_un_oferente,
       ROUND(SUM(promedio_oferentes * procesos_con_ofertas) / SUM(procesos_con_ofertas), 2) AS promedio_oferentes
FROM secop_trabajo2.competencia_departamento
GROUP BY departamento_entidad
HAVING SUM(procesos_con_ofertas) >= 1000
ORDER BY pct_un_oferente DESC
LIMIT 10;

-- 5. Pregunta rama 2: que tipos de contrato y modalidades concentran mas presupuesto,
--    y cuanto de ese presupuesto se contrata de forma directa?
SELECT tipo_de_contrato, modalidad_de_contratacion, num_procesos,
       presupuesto_miles_millones_cop, precio_mediano_millones_cop, es_contratacion_directa
FROM secop_trabajo2.presupuesto_modalidad
ORDER BY presupuesto_miles_millones_cop DESC
LIMIT 10;

-- 6. Rama 2, resumen: participacion de la contratacion directa en el presupuesto total
SELECT es_contratacion_directa,
       SUM(num_procesos) AS procesos,
       ROUND(SUM(presupuesto_miles_millones_cop), 1) AS presupuesto_miles_millones_cop,
       ROUND(100.0 * SUM(presupuesto_miles_millones_cop) / SUM(SUM(presupuesto_miles_millones_cop)) OVER (), 1) AS pct_presupuesto
FROM secop_trabajo2.presupuesto_modalidad
GROUP BY es_contratacion_directa;

-- 7. Datos transformados: registros por particion (anio_publicacion)
CREATE EXTERNAL TABLE IF NOT EXISTS secop_trabajo2.procesos (
  id_del_proceso string, entidad string, departamento_entidad string, modalidad_de_contratacion string,
  precio_base double, tipo_de_contrato string, num_proponentes int, tiene_competencia boolean
)
PARTITIONED BY (anio_publicacion int)
STORED AS PARQUET
LOCATION 's3://secop-trabajo2-equipo8-usw2/curated/procesos/';

MSCK REPAIR TABLE secop_trabajo2.procesos;

SELECT anio_publicacion, COUNT(*) AS procesos
FROM secop_trabajo2.procesos
GROUP BY anio_publicacion
ORDER BY anio_publicacion;
