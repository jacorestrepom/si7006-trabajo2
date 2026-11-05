# Trabajo 2: pipeline batch y streaming en AWS sobre SECOP II

- [Trabajo 2: pipeline batch y streaming en AWS sobre SECOP II](#trabajo-2-pipeline-batch-y-streaming-en-aws-sobre-secop-ii)
  - [Resumen](#resumen)
  - [Arquitectura](#arquitectura)
  - [Estructura del repositorio](#estructura-del-repositorio)
  - [Requisitos](#requisitos)
  - [Parte A: pipeline batch](#parte-a-pipeline-batch)
    - [A1. Datos de partida](#a1-datos-de-partida)
    - [A2. Bucket de S3 y carga de los datos](#a2-bucket-de-s3-y-carga-de-los-datos)
    - [A3. Trabajos de AWS Glue](#a3-trabajos-de-aws-glue)
    - [A4. Máquina de estados en AWS Step Functions](#a4-máquina-de-estados-en-aws-step-functions)
    - [A5. Horario de Amazon EventBridge Scheduler](#a5-horario-de-amazon-eventbridge-scheduler)
    - [A6. Consultas con Amazon Athena](#a6-consultas-con-amazon-athena)
    - [A7. Resultados de la parte A](#a7-resultados-de-la-parte-a)
  - [Parte B: streaming](#parte-b-streaming)
    - [B1. Stream de Kinesis](#b1-stream-de-kinesis)
    - [B2. Tablas de DynamoDB](#b2-tablas-de-dynamodb)
    - [B3. Lambdas consumidoras](#b3-lambdas-consumidoras)
    - [B4. Productor](#b4-productor)
    - [B5. Consultas y conteos](#b5-consultas-y-conteos)
    - [B6. Resultados de la parte B](#b6-resultados-de-la-parte-b)
  - [Evidencia de que no hay infraestructura como código](#evidencia-de-que-no-hay-infraestructura-como-código)
  - [Costos y limpieza](#costos-y-limpieza)
  - [Declaración de uso de IA](#declaración-de-uso-de-ia)

---

## Resumen

Equipo 8: David Lopera, Juan Diego Acuña, Nicolás Hurtado, José Alejandro Duque y Jacobo Restrepo.

Nuestro proyecto integrador es un **recomendador de contratos públicos**: a partir de los procesos de compra del Estado colombiano publicados en SECOP II, queremos mostrarle a cada empresa pequeña los procesos abiertos donde tiene más probabilidad de ganar. Este trabajo construye en AWS las dos piezas de datos que ese sistema necesita:

- **Parte A (batch)**: un horario de EventBridge Scheduler inicia cada día una máquina de Step Functions. Un trabajo de Glue (PySpark) lee **7.781.911 procesos** de SECOP II en CSV, los limpia, los une con los **2.362.926 registros de proponentes** (qué empresa presentó oferta en cada proceso) y escribe Parquet particionado por año. Después, dos trabajos de Glue en paralelo calculan dos agrupaciones que respondemos con Athena.
- **Parte B (streaming)**: un productor publica en Kinesis Data Streams procesos recién publicados (simulados), incluyendo repetidos e inválidos a propósito. Una Lambda calcula por ventanas de un minuto el conteo y el promedio del presupuesto por modalidad de contratación y lo guarda en DynamoDB; otra Lambda guarda en S3 todos los eventos crudos. Los conteos cuadran: publicados = únicos + repetidos + inválidos.

Todo se creó **a mano en la consola de AWS**, en la región **us-west-2 (Oregón)** de una cuenta de AWS Academy Learner Lab, usando el rol `LabRole`. No se usó CloudFormation, Terraform ni Serverless Framework.

## Arquitectura

```text
PARTE A (batch)
EventBridge Scheduler (cron diario, America/Bogota)
        │ StartExecution
        ▼
Step Functions: secop-batch-pipeline
  ┌──────────────────────────────┐
  │ Glue secop-clean-join        │  S3 raw/ (CSV)  ──►  S3 curated/procesos/ (Parquet, partición anio_publicacion)
  └──────────────┬───────────────┘
                 │ éxito
        ┌────────┴─────────┐   (estado Parallel)
        ▼                  ▼
 Glue secop-agg-       Glue secop-agg-
 competencia           modalidad
        │                  │
        ▼                  ▼
 S3 analytics/competencia_departamento/   S3 analytics/presupuesto_modalidad/   (Parquet)
        └──────────► Athena (base secop_trabajo2) ◄──────────┘

PARTE B (streaming)
producer.py ──► Kinesis Data Streams: secop-procesos (1 shard)
                    │                                  │
                    ▼                                  ▼
     Lambda secop-windows-processor          Lambda secop-raw-to-s3
       ├─ inválidos  → DynamoDB secop-invalid-events (con motivo)
       ├─ repetidos  → detectados con DynamoDB secop-seen-events (event_id)
       ├─ ventanas 1 min → DynamoDB secop-windows (conteo, suma, promedio)
       └─ totales    → DynamoDB secop-counters
                                              S3 stream/raw/dt=AAAA-MM-DD/*.jsonl
```

## Estructura del repositorio

| Ruta | Contenido |
| --- | --- |
| `scripts/download_secop.py` | Descarga los datos de datos.gov.co (API Socrata) por año |
| `glue/secop_clean_join.py` | Trabajo de Glue inicial: limpieza, join y Parquet particionado |
| `glue/secop_agg_competencia.py` | Rama 1: competencia por departamento y año |
| `glue/secop_agg_modalidad.py` | Rama 2: presupuesto por tipo de contrato y modalidad |
| `stepfunctions/secop-batch-pipeline.asl.json` | Definición de la máquina de estados (se importa en la consola) |
| `athena/queries.sql` | Tablas y consultas de Athena |
| `streaming/producer.py` | Productor de eventos hacia Kinesis |
| `streaming/lambda_windows.py` | Lambda de ventanas, repetidos e inválidos |
| `streaming/lambda_raw_to_s3.py` | Lambda que guarda los eventos crudos en S3 |
| `streaming/query_windows.py` | Consulta a DynamoDB que responde la pregunta de la parte B |
| `streaming/verify_counts.py` | Verificación de conteos |
| `streaming/reset_streaming.py` | Deja las tablas y `stream/raw/` vacías antes de una corrida |
| `results/` | Salidas reales de nuestras corridas (conteos, consultas) |

## Requisitos

- Una sesión de AWS Academy Learner Lab iniciada (botón **Start Lab**) y la consola abierta desde el enlace **AWS**. Toda la práctica se hace en **us-west-2**.
- Python 3.11 o superior con `boto3` para el productor y los scripts de consulta: `pip install boto3`.
- AWS CLI v2 configurado con las credenciales temporales del Learner Lab (**AWS Details → AWS CLI → Show**, copiadas en `~/.aws/credentials`) y `export AWS_DEFAULT_REGION=us-west-2`. Solo se usa para **subir los datos** a S3 y correr los scripts; ningún recurso se crea con el CLI.

Nombre del bucket usado: `secop-trabajo2-equipo8-usw2`. Si se reproduce en otra cuenta, cambiar ese nombre en el parámetro `--BUCKET` de los trabajos de Glue, en `athena/queries.sql` y en la variable `BUCKET` de la Lambda `secop-raw-to-s3`.

---

## Parte A: pipeline batch

### A1. Datos de partida

| Conjunto | Fuente | Formato | Registros |
| --- | --- | --- | --- |
| SECOP II - Procesos de Contratación, publicados 2022-2026 | datos.gov.co, `p6dx-8zbt` | CSV (5 archivos, uno por año) | **7.781.911** |
| SECOP II - Proponentes por Proceso | datos.gov.co, `hgi6-6wh3` | CSV | 2.362.926 |

Procesos por año: 2022: 1.038.590 · 2023: 1.531.550 · 2024: 1.670.371 · 2025: 1.934.951 · 2026: 1.606.449. No fue necesario aumentar los datos: el conjunto real supera los 5.000.000 de registros.

Para no publicar datos personales (Ley 1581 de 2012) solo se descargan columnas de la entidad y del proceso; de los proponentes se usa el código interno de SECOP del proveedor, no su nombre ni su NIT.

```bash
python scripts/download_secop.py procesos 2022   # repetir con 2023, 2024, 2025, 2026 (pueden correr en paralelo)
python scripts/download_secop.py proponentes
```

Resultado: `data/raw/procesos/procesos_AAAA.csv` y `data/raw/proponentes/proponentes.csv` (≈1,9 GB en total).

### A2. Bucket de S3 y carga de los datos

1. Consola de **S3 → Create bucket**: tipo *General purpose*, nombre `secop-trabajo2-equipo8-usw2`, región us-west-2, el resto por defecto.
2. Subir los CSV (por tamaño se usa el CLI; es carga de datos, no creación de recursos):

```bash
aws s3 cp data/raw/procesos/ s3://secop-trabajo2-equipo8-usw2/raw/procesos/ --recursive
aws s3 cp data/raw/proponentes/ s3://secop-trabajo2-equipo8-usw2/raw/proponentes/ --recursive
```

Zonas del bucket: `raw/` (CSV de entrada), `curated/procesos/` (Parquet limpio y unido), `analytics/` (agrupaciones), `athena-results/`, `stream/raw/` (parte B).

### A3. Trabajos de AWS Glue

Se crean tres trabajos en **AWS Glue → ETL jobs → Script editor → Spark → Upload script**, subiendo el archivo `.py` correspondiente. En **Job details** de cada uno:

| Campo | `secop-clean-join` | `secop-agg-competencia` | `secop-agg-modalidad` |
| --- | --- | --- | --- |
| Script | `glue/secop_clean_join.py` | `glue/secop_agg_competencia.py` | `glue/secop_agg_modalidad.py` |
| IAM role | `LabRole` | `LabRole` | `LabRole` |
| Glue version | 5.0 (Spark 3.5) | 5.0 | 5.0 |
| Worker type / número | G.1X / 5 | G.1X / 2 | G.1X / 2 |
| Job timeout | 60 min | 30 min | 30 min |
| Job parameters | `--BUCKET` = `secop-trabajo2-equipo8-usw2` | igual | igual |

El Learner Lab solo permite workers G.1X o Standard, máximo 10 workers y concurrencia 1 por trabajo.

**Qué hace el trabajo inicial (`secop_clean_join.py`)**

- **Lee** los CSV de procesos y proponentes desde `raw/`.
- **Corrige**: quita espacios repetidos; convierte a nulo los textos vacíos o "No definido"; pasa a nulo precios, valores adjudicados y duraciones negativos o en cero; normaliza el departamento a mayúsculas sin tildes (para que "Bogotá" y "BOGOTA" agrupen igual); convierte `adjudicado` a booleano; extrae el segmento UNSPSC del código de categoría.
- **Descarta**: registros sin identificador del proceso, sin fecha de publicación válida o fuera de 2022-2026; **duplicados** del mismo `id_del_proceso` (se conserva la fila más reciente); proponentes sin proceso o sin proveedor y proponentes repetidos en el mismo proceso.
- **Une (join)** por la columna común `id_del_proceso` = `id_procedimiento`: a cada proceso le agrega `num_proponentes` (empresas distintas que ofertaron) y `tiene_competencia` (2 o más oferentes). Es un *left join*: los procesos sin ofertas quedan con 0.
- **Escribe** Parquet en `curated/procesos/`, **particionado por `anio_publicacion`**.
- Imprime en el log de CloudWatch una línea `[conteos]` con leídos, descartados, duplicados y escritos.

**Por qué particionar por `anio_publicacion`**

1. Las consultas del proyecto se hacen por periodo: el modelo se reentrena con los procesos ya cerrados de un rango de años, y la ingesta diaria solo agrega datos al año en curso. Con esta partición Athena y Spark leen solo las carpetas del año que se pide (*partition pruning*).
2. Tiene pocos valores (5) y bien balanceados (entre 1,0 y 1,9 millones de filas por año), así que genera archivos grandes, sin el problema de miles de archivos pequeños que darían columnas como entidad o ciudad (miles de valores) o la fecha diaria (más de 1.700 carpetas).
3. Es una columna que nunca cambia para un proceso, así que una partición no se reescribe por actualizaciones de otra.

### A4. Máquina de estados en AWS Step Functions

1. **Step Functions → State machines → Create state machine → Create from blank**, nombre `secop-batch-pipeline`, tipo *Standard*.
2. **Actions → Import definition** y subir `stepfunctions/secop-batch-pipeline.asl.json`.
3. Pestaña **Config → Execution role**: `LabRole`. **Create**.

La definición tiene un estado `LimpiarYUnir` (Glue `startJobRun.sync`, que espera a que el trabajo termine) y, solo si termina con éxito, un estado `Parallel` con las dos ramas, cada una otro `startJobRun.sync`. Si el trabajo inicial falla, la ejecución falla y las ramas no se inician.

### A5. Horario de Amazon EventBridge Scheduler

1. **EventBridge → Scheduler → Schedules → Create schedule**, nombre `secop-batch-diario`.
2. *Recurring schedule*, zona horaria `America/Bogota`, *Cron-based*: `cron(42 15 * * ? *)` (todos los días a las 15:42), *Flexible time window*: Off.
3. Target: **AWS Step Functions StartExecution**, máquina `secop-batch-pipeline`, input `{}`.
4. Permissions: *Use existing role* → `LabRole`. **Create schedule**.

### A6. Consultas con Amazon Athena

1. **Athena → Query editor → Edit settings**: *Location of query result* = `s3://secop-trabajo2-equipo8-usw2/athena-results/`.
2. Ejecutar una a una las sentencias de `athena/queries.sql`: crean la base `secop_trabajo2` y las tablas externas sobre los Parquet, y responden las dos preguntas.

### A7. Resultados de la parte A

_Se completa con los resultados de la ejecución iniciada por el horario._

---

## Parte B: streaming

**Evento**: un proceso de contratación recién publicado.

```json
{"event_id": "uuid", "id_proceso": "CO1.REQ.12345678", "modalidad": "Contratación directa",
 "departamento": "ANTIOQUIA", "precio_base_millones": 85.3, "event_time": "2026-11-05T20:21:03.120+00:00"}
```

- **Categoría**: `modalidad` (modalidad de contratación). **Campo numérico**: `precio_base_millones` (presupuesto en millones de COP). Las proporciones y escalas de precio por modalidad imitan las del dataset real.
- **Evento repetido**: el que llega con un `event_id` ya recibido. Se reconoce con una escritura condicional (`attribute_not_exists(event_id)`) en la tabla `secop-seen-events`; si falla, el evento no se cuenta otra vez.
- **Evento inválido**: JSON mal formado; falta `event_id`, `modalidad`, `precio_base_millones` o `event_time`; precio no numérico o fuera del rango **(0, 50.000] millones de COP**; fecha inválida.
- **Ventana**: de un minuto (*tumbling*) según `event_time`, por modalidad.

### B1. Stream de Kinesis

**Kinesis → Data streams → Create data stream**: nombre `secop-procesos`, capacidad *Provisioned*, 1 shard.

### B2. Tablas de DynamoDB

**DynamoDB → Tables → Create table**, *Default settings* (on-demand):

| Tabla | Partition key | Sort key | Uso |
| --- | --- | --- | --- |
| `secop-windows` | `modalidad` (S) | `window_start` (S) | Resultado de cada ventana |
| `secop-seen-events` | `event_id` (S) | | Event ids ya procesados (repetidos) |
| `secop-invalid-events` | `sequence_number` (S) | | Eventos inválidos con su `motivo` |
| `secop-counters` | `metric` (S) | | Totales: únicos, repetidos, inválidos, recibidos |

### B3. Lambdas consumidoras

Para cada Lambda: **Lambda → Create function → Author from scratch**, runtime **Python 3.13**, *Additional settings → Custom execution role* = `LabRole`. Luego **Code → Update → Upload from .zip file** con un zip que contenga el `.py` renombrado a `lambda_function.py` (handler `lambda_function.lambda_handler`), y en **Configuration → General configuration** timeout de 1 minuto.

```bash
cd streaming && mkdir -p build/w build/r
cp lambda_windows.py build/w/lambda_function.py && (cd build/w && zip ../lambda_windows.zip lambda_function.py)
cp lambda_raw_to_s3.py build/r/lambda_function.py && (cd build/r && zip ../lambda_raw_to_s3.zip lambda_function.py)
```

| Lambda | Zip | Memoria | Trigger Kinesis (`secop-procesos`) |
| --- | --- | --- | --- |
| `secop-windows-processor` | `lambda_windows.zip` | 256 MB | batch size 100, batch window 5 s, starting position *Latest* |
| `secop-raw-to-s3` | `lambda_raw_to_s3.zip` | 128 MB | batch size 100, batch window 10 s, starting position *Latest* |

Las dos Lambdas son dos consumidores independientes del mismo stream: cada trigger lleva su propia posición de lectura.

Importante: crear los triggers **antes** de iniciar el productor y esperar un par de minutos a que queden en estado *Enabled*. Con *Latest*, los eventos publicados antes de que el trigger empiece a leer no se procesan.

### B4. Productor

```bash
cd streaming
python reset_streaming.py                       # tablas y stream/raw/ en cero
python producer.py --stream secop-procesos --minutes 6 --rate 5
```

Publica en lotes de 5 eventos por segundo (`PutRecords`) durante 6 minutos. Cerca del 7 % son repetidos (se reenvía un evento ya enviado, con el mismo `event_id` y la misma partition key) y cerca del 6 % son inválidos de cinco tipos. Al final escribe `producer_counts.json` con lo publicado.

### B5. Consultas y conteos

- Consola **DynamoDB → Explore items → secop-windows** o **PartiQL editor**: `SELECT * FROM "secop-windows" WHERE modalidad = 'Licitación pública'`.
- `python query_windows.py`: lista cada ventana y responde la pregunta.
- `python verify_counts.py`: compara lo publicado con lo que contó la Lambda y con los eventos crudos en S3.
- Eventos inválidos: consola **DynamoDB → secop-invalid-events** (columnas `motivo` y `payload`).
- Eventos crudos: consola **S3 → secop-trabajo2-equipo8-usw2 → stream/raw/**.

### B6. Resultados de la parte B

Corrida del 5 de noviembre de 2026 (salidas completas en `results/`):

```text
Productor  -> publicados=1400 (unicos=1224, repetidos=86, invalidos=90)
Lambda     -> unicos_procesados=1224  repetidos_descartados=86  invalidos_descartados=90  recibidos=1400
S3 crudos  -> 1400 eventos   |   secop-invalid-events -> 90 filas

1400 publicados = 1224 + 86 + 90 = 1400  ->  CUADRA
```

Inválidos por tipo: JSON mal formado 21, precio negativo 21, sin modalidad 19, sin `event_id` 15, precio por encima del máximo 14.

**Pregunta del proyecto**: ¿qué modalidad de contratación publica más procesos por minuto y cuál tiene el mayor presupuesto promedio? Es decir, ¿dónde aparecen más oportunidades nuevas que el recomendador debe revisar con más frecuencia y dónde están los contratos grandes?

| Modalidad | Eventos | Eventos/min | Promedio (M COP) |
| --- | --- | --- | --- |
| Contratación directa | 765 | 109,3 | 105,74 |
| Contratación régimen especial | 166 | 23,7 | 404,95 |
| Mínima cuantía | 160 | 22,9 | 20,79 |
| Selección abreviada de menor cuantía | 54 | 7,7 | 692,18 |
| Licitación pública | 42 | 6,0 | 5.032,25 |
| Concurso de méritos abierto | 37 | 5,3 | 1.559,54 |

Respuesta: la contratación directa concentra la mayoría de los procesos nuevos (más de 100 por minuto en la simulación), pero sus montos son bajos. Las licitaciones públicas son pocas (6 por minuto) y tienen el presupuesto promedio más alto, unas 48 veces el de la contratación directa. Para una MiPyme, las oportunidades frecuentes y accesibles están en mínima cuantía y contratación directa, y el recomendador debe priorizar las alertas de licitación por su valor.

---

## Evidencia de que no hay infraestructura como código

La consola de **CloudFormation en us-west-2** no tiene ningún stack. La región us-east-1 de la cuenta conserva el stack que crea el propio Learner Lab al iniciar (el que define `LabRole`); no se borra porque la cuenta dejaría de funcionar, y por eso todo el trabajo se hizo en us-west-2.

## Costos y limpieza

El Learner Lab tiene un presupuesto de USD 50. Lo que más cuesta es Glue (USD 0,44 por DPU-hora) y el shard de Kinesis (USD 0,015 por hora). Al terminar la sustentación:

1. EventBridge Scheduler: borrar `secop-batch-diario` (si no, ejecuta el pipeline todos los días).
2. Kinesis: borrar `secop-procesos`.
3. Lambda: borrar los triggers y las funciones `secop-windows-processor` y `secop-raw-to-s3`.
4. DynamoDB: borrar las 4 tablas.
5. Step Functions: borrar `secop-batch-pipeline`. Glue: borrar los 3 trabajos.
6. Athena: `DROP DATABASE secop_trabajo2 CASCADE;`
7. S3: vaciar y borrar `secop-trabajo2-equipo8-usw2`.

## Declaración de uso de IA

_Se completa al final._
