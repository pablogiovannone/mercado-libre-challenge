import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import com.mercadolibre.challenge.PurchaseDataProcessorUtils._

// Caso de clase para representar los datos de compra
case class Purchase(
  user_id: java.lang.Integer,
  user_name: String,
  email: String,
  purchase_datetime: String,
  purchase_amount: Double
)

// Suite de pruebas para validar la lógica del procesamiento de datos
class PurchaseDataProcessorSpec extends AnyFunSuite {

  // Prueba con datos válidos: el procesamiento completo debe devolver agregados correctos
  test("testValidInputProcessing") {
    val spark = SparkSession.builder().appName("testValidInputProcessing").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      Purchase(1, "user1", "email1@gmail.com", "01/15/2023 10:00:00 AM", 100.0),
      Purchase(2, "user2", "email2@gmail.com", "06/20/2023 04:30:00 PM", 200.0)
    )
    val df = spark.createDataset(data).toDF()

    val cleaned = cleanNulls(df)
    val dateCleaned = cleanDatetime(cleaned)
    val parsed = parseDatetime(dateCleaned)
    val filtered = filterDateRange(parsed)
    val aggregated = aggregateByUser(filtered)

    val result = aggregated.collect()
    assert(result.length == 2)
    assert(result.exists(row => row.getInt(0) == 1 && row.getDouble(1) == 100.0))
    assert(result.exists(row => row.getInt(0) == 2 && row.getDouble(1) == 200.0))

    spark.stop()
  }

  // Prueba que valida que registros con valores nulos sean eliminados correctamente
  test("testNullValuesAreFiltered") {
    val spark = SparkSession.builder().appName("testNullValuesAreFiltered").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      Purchase(1, "user1", "email1@gmail.com", "01/15/2023 10:00:00 AM", 100.0),
      Purchase(null, "user2", "email2@gmail.com", "06/20/2023 04:30:00 PM", 200.0),
      Purchase(3, "user3", null, "06/20/2023 04:30:00 PM", 300.0),
      Purchase(4, "user4", "email4@gmail.com", null, 400.0)
    )
    val df = spark.createDataset(data).toDF()

    val cleaned = cleanNulls(df)
    val result = cleaned.collect()
    assert(result.length == 1)
    assert(result.head.getInt(0) == 1)

    spark.stop()
  }

  // Prueba que valida que fechas mal formateadas sean ignoradas en el procesamiento
  test("testInvalidDateFormatIsIgnored") {
    val spark = SparkSession.builder().appName("testInvalidDateFormatIsIgnored").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      Purchase(1, "user1", "email1@gmail.com", "2023/01/01 10:00:00", 100.0), // mal formato
      Purchase(2, "user2", "email2@gmail.com", "01/01/2023 10:00:00 AM", 200.0) // correcto
    )
    val df = spark.createDataset(data).toDF()

    val cleaned = cleanNulls(df)
    val dateCleaned = cleanDatetime(cleaned)
    val parsed = parseDatetime(dateCleaned)
    val filtered = filterDateRange(parsed)
    val aggregated = aggregateByUser(filtered)

    val result = aggregated.collect()
    assert(result.length == 1)
    assert(result.head.getInt(0) == 2)

    spark.stop()
  }

  // Prueba con dataset vacío: debe retornar un DataFrame vacío sin errores
  test("testEmptyDatasetReturnsEmpty") {
    val spark = SparkSession.builder().appName("testEmptyDatasetReturnsEmpty").master("local[*]").getOrCreate()

    val schema = StructType(Seq(
      StructField("user_id", IntegerType, true),
      StructField("user_name", StringType, true),
      StructField("email", StringType, true),
      StructField("purchase_datetime", StringType, true),
      StructField("purchase_amount", DoubleType, true)
    ))

    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val result = aggregateByUser(filterDateRange(parseDatetime(cleanDatetime(cleanNulls(emptyDF)))))

    assert(result.count() == 0)

    spark.stop()
  }

  // Prueba con todos los registros inválidos: el resultado debe estar vacío
  test("testAllInvalidReturnsEmpty") {
    val spark = SparkSession.builder().appName("testAllInvalidReturnsEmpty").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      Purchase(null, "user1", "email1@gmail.com", "01/15/2023 10:00:00 AM", 100.0),
      Purchase(2, "user2", null, "06/20/2023 04:30:00 PM", 200.0),
      Purchase(3, "user3", "email3@gmail.com", null, 300.0)
    )
    val df = spark.createDataset(data).toDF()

    val cleaned = cleanNulls(df)
    assert(cleaned.count() == 0)

    spark.stop()
  }
}
