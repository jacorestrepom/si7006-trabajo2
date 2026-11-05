# Guion del video (máximo 12 minutos)

Reglas que eliminan el trabajo si no se cumplen: **dura 12 minutos o menos**, **hablan los 5 integrantes con la cámara encendida** mientras hablan, y se muestra **CloudFormation sin stacks en us-west-2**. No mostrar la pestaña del Learner Lab con *AWS Details* abierto ni ninguna credencial.

Antes de grabar:

- Iniciar el Learner Lab, abrir la consola y confirmar que la región es **United States (Oregon) us-west-2**.
- Zoom del navegador al 125 % o 150 % para que las tablas se lean.
- Dejar abiertas en pestañas: CloudFormation, S3, Glue (jobs), Step Functions (la ejecución del horario), EventBridge Scheduler, Athena, Kinesis, Lambda, DynamoDB, CloudWatch Logs.
- Parte B en vivo: correr `python reset_streaming.py` antes de grabar ese bloque. El productor dura 6 minutos, así que se arranca al iniciar la parte B y se muestran los resultados al final.

| Tiempo | Quién | Qué se muestra | Qué se dice |
| --- | --- | --- | --- |
| 0:00-0:45 | Jacobo | Cámara + diagrama del README | El proyecto (recomendador de contratos de SECOP II), las dos partes del trabajo, la región us-west-2 |
| 0:45-1:15 | Jacobo | **CloudFormation us-west-2: Stacks (0)** | Todo se creó a mano en la consola; no hay stacks. En us-east-1 solo queda el stack del Learner Lab que crea LabRole |
| 1:15-2:15 | David | S3 `raw/procesos` y `raw/proponentes`; la tabla de A1 del README | Los datos de partida: 7.781.911 procesos (2022-2026) en 5 CSV y 2.362.926 proponentes; fuente datos.gov.co; sin datos personales |
| 2:15-3:30 | David | Script `secop_clean_join.py` en el editor de Glue | Lectura de CSV, correcciones, descartes (sin id, sin fecha, duplicados), join por `id_del_proceso`, escritura en Parquet por `anio_publicacion` |
| 3:30-4:15 | Juan Diego | S3 `curated/procesos/anio_publicacion=2022 … 2026` y log `[conteos]` en CloudWatch | Partición por año: consultas por periodo, reentreno con años cerrados, 5 particiones balanceadas, sin archivos pequeños |
| 4:15-5:15 | Juan Diego | EventBridge Scheduler `secop-batch-diario` (cron) → Step Functions, ejecución iniciada por el horario, grafo en verde | El horario inicia la máquina; el Glue inicial y, al terminar con éxito, las dos ramas en paralelo; la ejecución terminó con éxito (mostrar hora de inicio = hora del cron) |
| 5:15-6:30 | Nicolás | S3 `analytics/`, Athena: consultas 4, 5 y 6 de `queries.sql` | Rama 1: dónde hay menos competencia. Rama 2: dónde está el presupuesto y cuánto es contratación directa. Leer en voz alta la respuesta |
| 6:30-7:30 | José Alejandro | Kinesis `secop-procesos`; terminal con `producer.py` publicando | El evento, la categoría (modalidad) y el número (presupuesto); repetidos e inválidos a propósito |
| 7:30-8:45 | José Alejandro | Lambda `secop-windows-processor` (código y trigger) | Validación, repetidos por `event_id` con escritura condicional, ventanas de 1 minuto por `event_time`, promedio = suma/conteo |
| 8:45-9:45 | Nicolás | DynamoDB `secop-windows` (Explore items) y PartiQL; `python query_windows.py` | Resultado por ventana y la respuesta a la pregunta |
| 9:45-10:30 | Jacobo | DynamoDB `secop-invalid-events` (columna motivo); S3 `stream/raw/` con un `.jsonl` abierto; Lambda `secop-raw-to-s3` | Inválidos apartados con su motivo; segundo consumidor que guarda todo crudo |
| 10:30-11:30 | Jacobo | `python verify_counts.py` y tabla `secop-counters` | Publicados = únicos + repetidos + inválidos, y también = eventos crudos en S3 |
| 11:30-12:00 | Todos | Cámara | Cierre y declaración breve de uso de IA |

Ensayar una vez con cronómetro; si se pasa, recortar la explicación del código de la Lambda.
