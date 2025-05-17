

**Technical Challenge DE**

**Giovannone Pablo**

**16/05/2025**

[**Technical Challenge for Data Engineer Candidate	3**](#technical-challenge-for-data-engineer-candidate)

[Part 1: Data Processing Task	3](#part-1:-data-processing-task)

[Part 2: Data-Driven Testing	4](#part-2:-data-driven-testing)

[Part 3: Fault Tolerance	4](#part-3:-fault-tolerance)

[Part 4: Performance Troubleshooting	5](#part-4:-performance-troubleshooting)

[Coding Skills	5](#coding-skills)

[Testing Ability	5](#testing-ability)

[**Part1	6**](#part1)

[**🔧 Requisitos:	6**](#🔧-requisitos:)

[**📌 Pasos rápidos:	6**](#📌-pasos-rápidos:)

[Comenzando	8](#comenzando)

[💻Compilación y ejecución del script en local	12](#💻compilación-y-ejecución-del-script-en-local)

[☁️Publicación en la nube (Google Cloud Storage)	14](#☁️publicación-en-la-nube-\(google-cloud-storage\))

# Technical Challenge for Data Engineer Candidate  {#technical-challenge-for-data-engineer-candidate}

### Objective 

Showcase your ability to process data, handle fault tolerance, implement data-driven testing, and troubleshoot performance issues in a distributed data processing application using Scala and Apache Spark. 

### Challenge 

## Part 1: Data Processing Task  {#part-1:-data-processing-task}

#### Input Data 

Link to .csv with columns: 

● user\_id (Integer) 

● user\_name (String) 

● email (String) 

● purchase\_datetime (String/Date) 

● purchase\_amount (Float) 

#### Instructions 

1\. Write a Scala application using Apache Spark to read the CSV file. 

2\. Process the data with the following steps: 

a. Clean the dataset by removing any records with null values in critical columns (user\_id, email, purchase\_datetime). 

b. Convert the purchase\_datetime column to a proper timestamp format. c. Filter the records to include only those within a specific date range (e.g., between '2023-01-01' and '2023-12-31'). 

d. Aggregate the data to compute the total purchase\_amount per user\_id. 

3\. Save the processed data as Parquet files to a Cloud Storage location of your preference (e.g., AWS S3, Google Cloud Storage). 

#### Deliverable 

Scala code in an accessible GitHub repository implementing the above logic. 

## Part 2: Data-Driven Testing  {#part-2:-data-driven-testing}

#### Instructions

Create unit tests for the data processing logic using a Scala testing framework (e.g., ScalaTest or MUnit). Your tests should cover: 

● Valid inputs: verifying that the outputs match expected results for correctly formatted and complete CSV records. 

● Invalid inputs: verifying that records with null values or incorrect date formats are handled appropriately (e.g., are removed or trigger exceptions). 

● Boundary cases: such as dealing with an empty CSV file and ensuring the application handles it gracefully. 

#### Deliverable 

Scala test cases in the same GitHub repository as part 1 covering the processing logic described above. 

## Part 3: Fault Tolerance  {#part-3:-fault-tolerance}

#### Instructions 

Describe resiliency patterns you would implement to ensure fault tolerance in the distributed Spark job. Consider aspects such as: 

1\. Handling node failures during processing. 

2\. Strategies for retrying failed tasks automatically. 

3\. The use of checkpointing mechanisms to save processing state and recover from interruptions. 

4\. Any additional strategies to minimize downtime in the event of transient issues. 

#### Deliverable 

A brief report (1-2 pages) explaining your fault tolerance strategies for the Spark application. 

## 

## Part 4: Performance Troubleshooting  {#part-4:-performance-troubleshooting}

#### Instructions 

Present a scenario where the Spark job takes significantly longer to process than expected. 

1\. Provide a list of at least 5 performance troubleshooting questions you would ask the team along with an explanation for each. 

2\. Describe actions you would take to diagnose performance bottlenecks 

#### Deliverable 

A document listing performance troubleshooting questions, their rationale, and proposed diagnostic actions. 

### Evaluation Criteria

## Coding Skills  {#coding-skills}

Clarity, efficiency, and structure of the Scala code. The code must correctly implement the CSV to Parquet transformation, taking into account cleansing, formatting, filtering, and aggregation. 

## Testing Ability  {#testing-ability}

Comprehensive coverage and effectiveness of the data-driven tests written using Scala’s testing frameworks. 

Fault Tolerance Understanding 

Depth of knowledge regarding resiliency patterns for distributed systems and a clear explanation of strategies to handle failures in Spark. 

Performance Consideration 

Insightfulness and thoroughness in the performance troubleshooting approach, showcasing the candidate's ability to identify and resolve issues in distributed data processing.

# Part1 {#part1}

## 🔧 Requisitos: {#🔧-requisitos:}

* **Java JDK 8 o 11** (instalado y en `PATH`)

* **Scala**

* **Apache Spark** 

## 📌 Pasos rápidos: {#📌-pasos-rápidos:}

1. Descargá e instalá Java JDK si no lo tenés (versión 8 u 11).	[https://adoptium.net/en-GB/temurin/releases/?version=11](https://adoptium.net/en-GB/temurin/releases/?version=11)  
2. Instalá Scala  
   [https://www.scala-lang.org/download/](https://www.scala-lang.org/download/)  
3. Descarga y descomprimir SPARK  
   [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)  
4. Agregar las variables de entorno JAVA\_HOME, PATH a las rutas correspondientes  
5. Descargar winutils y agregarlo al path HADOOP\_HOME  
   [https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0](https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0)  
6. Descargar sbt y agregarlo al PATH  
   [https://www.scala-sbt.org/download/](https://www.scala-sbt.org/download/)  
7. Validamos las instalaciones

| PS C:\\Users\\pablo\> java \-versionopenjdk version "11.0.27" 2025-04-15OpenJDK Runtime Environment Temurin-11.0.27+6 (build 11.0.27+6)OpenJDK 64-Bit Server VM Temurin-11.0.27+6 (build 11.0.27+6, mixed mode)PS C:\\Users\\pablo\> spark-shell25/05/13 19:33:01 WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException: java.io.FileNotFoundException: HADOOP\_HOME and hadoop.home.dir are unset. \-see https://wiki.apache.org/hadoop/WindowsProblems Setting default log level to "WARN". To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel). 25/05/13 19:33:05 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable Spark context Web UI available at http://Pablo.mshome.net:4040 Spark context available as 'sc' (master \= local\[\*\], app id \= local-1747175586277). Spark session available as 'spark'. Welcome to       \_\_\_\_              \_\_      / \_\_/\_\_  \_\_\_ \_\_\_\_\_/ /\_\_     \_\\ \\/ \_ \\/ \_ \`/ \_\_/  '\_/    /\_\_\_/ .\_\_/\\\_,\_/\_/ /\_/\\\_\\   version 3.5.5       /\_/ Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 11.0.27) Type in expressions to have them evaluated. Type :help for more information. scala\> |
| :---- |

###  

### Instructions 

1\. Write a Scala application using Apache Spark to read the CSV file. 

2\. Process the data with the following steps: 

a. Clean the dataset by removing any records with null values in critical columns (user\_id, email, purchase\_datetime). 

b. Convert the purchase\_datetime column to a proper timestamp format. c. Filter the records to include only those within a specific date range (e.g., between '2023-01-01' and '2023-12-31'). 

d. Aggregate the data to compute the total purchase\_amount per user\_id. 

3\. Save the processed data as Parquet files to a Cloud Storage location of your preference (e.g., AWS S3, Google Cloud Storage). 

## Comenzando {#comenzando}

A continuación, comenzaremos con la ejecución del script en un entorno interactivo utilizando **spark-shell**, lo que nos permitirá validar el procesamiento paso a paso de manera local. Una vez verificado el comportamiento esperado, procederemos con su compilación para una ejecución más estructurada o en un entorno productivo.

| { import org.apache.spark.sql.SparkSession import org.apache.spark.sql.functions.\_ import org.apache.log4j.{Level, Logger}    // Configuración de logging    val logger \= Logger.getLogger("PurchaseDataProcessor")    logger.setLevel(Level.INFO)    // Inicialización de Spark    val spark \= SparkSession.builder()      .appName("Purchase Data Processor")      .master("local\[\*\]")      .getOrCreate()    logger.info("=== 1\. Lectura del CSV \===")    val inputPath \= "F:/Trabajo/Mercadolibre/Examen/Purchases\_data-data.csv"    val outputPath \= "F:/Trabajo/Mercadolibre/Examen/output\_parquet"    val rawDf \= spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)    logger.info(s"Total registros leídos: ${rawDf.count()}")    rawDf.show(5, false)    logger.info("=== 2a. Limpieza de datos nulos \===")    val cleanedDf \= rawDf      .filter(col("user\_id").isNotNull && col("email").isNotNull && col("purchase\_datetime").isNotNull)    logger.info(s"Registros después de limpieza: ${cleanedDf.count()}")    logger.info("=== Limpieza y estandarización del campo de fecha \===")    val cleanedDatesDf \= cleanedDf      .withColumn("purchase\_datetime\_clean",        regexp\_replace(          regexp\_replace(            regexp\_replace(col("purchase\_datetime"), "\[^\\\\x00-\\\\x7F\]", ""), // Elimina caracteres invisibles (espacios no separables)            "\\\\.", ""),                                                    // Elimina puntos de "a.m." / "p.m."          "(?i)am", "AM"))      .withColumn("purchase\_datetime\_clean",        regexp\_replace(col("purchase\_datetime\_clean"), "(?i)pm", "PM"))    cleanedDatesDf.select("purchase\_datetime", "purchase\_datetime\_clean").show(5, false)    logger.info("=== 2b. Conversión de string a timestamp y extracción de fecha \===")    val withTimestampDf \= cleanedDatesDf      .withColumn("purchase\_datetime\_parsed",        to\_timestamp(col("purchase\_datetime\_clean"), "MM/dd/yyyy hh:mm:ss a"))      .withColumn("purchase\_date",        date\_format(col("purchase\_datetime\_parsed"), "yyyy-MM-dd"))    val parsedCount \= withTimestampDf.filter(col("purchase\_datetime\_parsed").isNotNull).count()    logger.info(s"Fechas parseadas correctamente: $parsedCount")    withTimestampDf.select("purchase\_datetime\_clean", "purchase\_datetime\_parsed", "purchase\_date").show(5, false)    logger.info("=== 2c. Filtrado por fechas en el rango 2023 \===")    val filteredDf \= withTimestampDf      .filter(col("purchase\_date").between("2023-01-01", "2023-12-31"))    logger.info(s"Registros dentro del rango 2023: ${filteredDf.count()}")    filteredDf.select("user\_id", "purchase\_date").show(5, false)    logger.info("=== 2d. Agregación de compras por usuario \===")    val aggregatedDf \= filteredDf      .groupBy("user\_id")      .agg(sum("purchase\_amount").alias("total\_purchase\_amount"))    logger.info(s"Cantidad de usuarios con compras en 2023: ${aggregatedDf.count()}")    aggregatedDf.show(20, false)    logger.info("=== 3\. Guardado del resultado en formato Parquet \===")    aggregatedDf.write.mode("overwrite").parquet(outputPath)    logger.info(s"Resultado guardado en: $outputPath")    logger.info("=== FIN DEL PROCESAMIENTO \===")    spark.stop() } |
| :---- |

#### Resultado: 

| \+-------+----------------+--------------------------+-------------------------+---------------+ |user\_id|user\_name       |email                     |purchase\_datetime        |purchase\_amount| \+-------+----------------+--------------------------+-------------------------+---------------+ |1      |Natalia González|natalia.gonzalez@gmail.com|02/02/2024 06:48:47 a. m.|71.95          | |2      |Isabel Rodríguez|isabel.rodriguez@gmail.com|02/03/2024 04:42:19 a. m.|1.02           | |3      |Diego López     |diego.lopez@gmail.com     |10/15/2022 07:12:52 a. m.|7.29           | |4      |Daniel Martínez |daniel.martinez@gmail.com |06/20/2023 03:13:19 p. m.|7974.08        | |5      |Ana Pérez       |ana.perez@gmail.com       |03/30/2023 12:25:29 p. m.|27.06          | \+-------+----------------+--------------------------+-------------------------+---------------+ only showing top 5 rows \+-------------------------+-----------------------+ |purchase\_datetime        |purchase\_datetime\_clean| \+-------------------------+-----------------------+ |02/02/2024 06:48:47 a. m.|02/02/2024 06:48:47 AM | |02/03/2024 04:42:19 a. m.|02/03/2024 04:42:19 AM | |10/15/2022 07:12:52 a. m.|10/15/2022 07:12:52 AM | |06/20/2023 03:13:19 p. m.|06/20/2023 03:13:19 PM | |03/30/2023 12:25:29 p. m.|03/30/2023 12:25:29 PM | \+-------------------------+-----------------------+ only showing top 5 rows \+-----------------------+------------------------+-------------+ |purchase\_datetime\_clean|purchase\_datetime\_parsed|purchase\_date| \+-----------------------+------------------------+-------------+ |02/02/2024 06:48:47 AM |2024-02-02 06:48:47     |2024-02-02   | |02/03/2024 04:42:19 AM |2024-02-03 04:42:19     |2024-02-03   | |10/15/2022 07:12:52 AM |2022-10-15 07:12:52     |2022-10-15   | |06/20/2023 03:13:19 PM |2023-06-20 15:13:19     |2023-06-20   | |03/30/2023 12:25:29 PM |2023-03-30 12:25:29     |2023-03-30   | \+-----------------------+------------------------+-------------+ only showing top 5 rows \+-------+-------------+ |user\_id|purchase\_date| \+-------+-------------+ |4      |2023-06-20   | |5      |2023-03-30   | |6      |2023-01-16   | |7      |2023-06-19   | |9      |2023-12-04   | \+-------+-------------+ only showing top 5 rows \+-------+---------------------+ |user\_id|total\_purchase\_amount| \+-------+---------------------+ |148    |1032546.7499999998   | |243    |1310870.1200000003   | |392    |935438.53            | |31     |1319832.7799999998   | |85     |1139286.5            | |251    |1144673.8            | |137    |930255.1199999998    | |65     |1245742.1700000004   | |53     |1049340.4            | |255    |902818.0700000001    | |133    |1163337.77           | |296    |1174799.03           | |78     |995666.39            | |322    |1105702.9500000002   | |362    |1198642.69           | |321    |1125672.2900000003   | |375    |1047932.2899999999   | |108    |922810.18            | |155    |1170736.6599999997   | |34     |1186480.3800000001   | \+-------+---------------------+ only showing top 20 rows  |
| :---- |

## 

## 💻Compilación y ejecución del script en local {#💻compilación-y-ejecución-del-script-en-local}

Una vez realizada la validación preliminar del procesamiento en **spark-shell**, procedemos a compilar el proyecto como una aplicación completa en Scala, utilizando **sbt** como herramienta de construcción.

#### La estructura del proyecto es la siguiente: 

MercadoLibreChallenge/  
├── build.sbt  
├── project/  
│└── build.properties  
└── src/  
    └── main/  
        └── scala/  
            └── PurchaseDataProcessor.scala

####  Configuración del entorno

La configuración de versiones y dependencias fue tomada a partir de los paquetes descargados previamente:

| sbt.version=1.10.11 |
| :---- |

build.sbt

| name := "mercado-libre-challenge" version := "0.1" scalaVersion := "2.12.18" libraryDependencies \++= Seq(   "org.apache.spark" %% "spark-core" % "3.5.5",   "org.apache.spark" %% "spark-sql" % "3.5.5" ) |
| :---- |

Si la compilación se realiza correctamente, el artefacto .jar resultante estará disponible en la siguiente ruta:   
target/scala-2.12/mercado-libre-challenge\_2.12-0.1.jar

Ejecutamos el siguiente comando, para validar que funcione correctamente

| spark-submit \--class PurchaseDataProcessor \--master local target/scala-2.12/mercado-libre-challenge\_2.12-0.1.jar |
| :---- |

#### Validación del resultado

Una vez finalizada la ejecución, verificamos el resultado accediendo al directorio donde se guardaron los archivos Parquet. En este caso, realizamos la validación desde **`spark-shell`:**

| { val df \= spark.read.parquet("F:/Trabajo/Mercadolibre/Examen/output\_parquet") df.printSchema() df.show(false) } |
| :---- |

#### Resultado: 

| \+-------+---------------------+ |user\_id|total\_purchase\_amount| \+-------+---------------------+ |148    |1032546.7499999998   | |243    |1310870.1200000003   | |392    |935438.53            | |31     |1319832.7799999998   | |85     |1139286.5            | |251    |1144673.8            | |137    |930255.1199999998    | |65     |1245742.1700000004   | |53     |1049340.4            | |255    |902818.0700000001    | |133    |1163337.77           | |296    |1174799.03           | |78     |995666.39            | |322    |1105702.9500000002   | |362    |1198642.69           | |321    |1125672.2900000003   | |375    |1047932.2899999999   | |108    |922810.18            | |155    |1170736.6599999997   | |34     |1186480.3800000001   | \+-------+---------------------+ only showing top 20 rows |
| :---- |

## ☁️Publicación en la nube (Google Cloud Storage) {#☁️publicación-en-la-nube-(google-cloud-storage)}

Para garantizar persistencia y disponibilidad, se decidió almacenar también los resultados en Google Cloud Storage (GCS), dado que ya se contaba con una cuenta configurada.  
Los pasos realizados fueron:

1. Crear un proyecto en Google Cloud.

2. Crear un bucket.

3. Configurar una Service Account con el rol Storage Object Admin.

4. Generar una clave de acceso en formato JSON.

Para mantener una separación clara entre la ejecución local y en la nube, se creó un segundo archivo **.scala** denominado **PurchaseDataProcessor\_GC.scala**, el cual incluye las siguientes modificaciones:

* Nuevo nombre de objeto y salida en GCS:

| object PurchaseDataProcessorGCval outputPath \= "gs://mercadolibre-bucket/output\_parquet" |
| :---- |

#### Ejecución en Google Cloud Storage

La ejecución del script adaptado para GCS se realiza con el siguiente comando, incluyendo las configuraciones necesarias para autenticación y acceso al bucket:

| spark-submit \--jars file:///F:/Trabajo/Mercadolibre/Examen/lib/gcs-connector-hadoop3-latest.jar \--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \--conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \--conf spark.hadoop.google.cloud.auth.service.account.enable=true \--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=F:/Trabajo/Mercadolibre/Examen/mercadolibre-key.json \--class PurchaseDataProcessorGC \--master local target/scala-2.12/mercado-libre-challenge\_2.12-0.1.jar |
| :---- |

Finalmente, se validó nuevamente el contenido de salida descargando los archivos desde GCS o volviendo a leerlos desde Spark. Alternativamente, esta validación puede realizarse directamente conectándose a Google Cloud.