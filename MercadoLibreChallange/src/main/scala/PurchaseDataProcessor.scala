import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object PurchaseDataProcessor {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("PurchaseDataProcessor")
    logger.setLevel(Level.INFO)

    val spark = SparkSession.builder()
      .appName("Purchase Data Processor")
      .master("local[*]")
      .getOrCreate()

    logger.info("=== 1. Lectura del CSV ===")
    val inputPath = "F:/Trabajo/Mercadolibre/Examen/Purchases_data-data.csv"
    val outputPath = "F:/Trabajo/Mercadolibre/Examen/output_parquet"
    val rawDf = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)
    logger.info(s"Total registros leídos: ${rawDf.count()}")
    rawDf.show(5, false)

    logger.info("=== 2a. Limpieza de datos nulos ===")
    val cleanedDf = rawDf.filter(col("user_id").isNotNull && col("email").isNotNull && col("purchase_datetime").isNotNull)
    logger.info(s"Registros después de limpieza: ${cleanedDf.count()}")

    logger.info("=== Limpieza y estandarización del campo de fecha ===")
    val cleanedDatesDf = cleanedDf
      .withColumn("purchase_datetime_clean",
        regexp_replace(
          regexp_replace(
            regexp_replace(col("purchase_datetime"), "[^\\x00-\\x7F]", ""),
            "\\.", ""),
          "(?i)am", "AM"))
      .withColumn("purchase_datetime_clean",
        regexp_replace(col("purchase_datetime_clean"), "(?i)pm", "PM"))

    cleanedDatesDf.select("purchase_datetime", "purchase_datetime_clean").show(5, false)

    logger.info("=== 2b. Conversión de string a timestamp y extracción de fecha ===")
    val withTimestampDf = cleanedDatesDf
      .withColumn("purchase_datetime_parsed",
        to_timestamp(col("purchase_datetime_clean"), "MM/dd/yyyy hh:mm:ss a"))
      .withColumn("purchase_date",
        date_format(col("purchase_datetime_parsed"), "yyyy-MM-dd"))

    val parsedCount = withTimestampDf.filter(col("purchase_datetime_parsed").isNotNull).count()
    logger.info(s"Fechas parseadas correctamente: $parsedCount")
    withTimestampDf.select("purchase_datetime_clean", "purchase_datetime_parsed", "purchase_date").show(5, false)

    logger.info("=== 2c. Filtrado por fechas en el rango 2023 ===")
    val filteredDf = withTimestampDf.filter(col("purchase_date").between("2023-01-01", "2023-12-31"))
    logger.info(s"Registros dentro del rango 2023: ${filteredDf.count()}")
    filteredDf.select("user_id", "purchase_date").show(5, false)

    logger.info("=== 2d. Agregación de compras por usuario ===")
    val aggregatedDf = filteredDf.groupBy("user_id").agg(sum("purchase_amount").alias("total_purchase_amount"))
    logger.info(s"Cantidad de usuarios con compras en 2023: ${aggregatedDf.count()}")
    aggregatedDf.show(20, false)

    logger.info("=== 3. Guardado del resultado en formato Parquet ===")
    aggregatedDf.write.mode("overwrite").parquet(outputPath)
    logger.info(s"Resultado guardado en: $outputPath")

    logger.info("=== FIN DEL PROCESAMIENTO ===")
    spark.stop()
  }
}
