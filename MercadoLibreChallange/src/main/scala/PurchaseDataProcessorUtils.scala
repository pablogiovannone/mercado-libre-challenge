package com.mercadolibre.challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PurchaseDataProcessorUtils {

  def cleanNulls(df: DataFrame): DataFrame = {
    df.filter(col("user_id").isNotNull && col("email").isNotNull && col("purchase_datetime").isNotNull)
  }

  def cleanDatetime(df: DataFrame): DataFrame = {
    df.withColumn("purchase_datetime_clean",
      regexp_replace(
        regexp_replace(
          regexp_replace(col("purchase_datetime"), "[^\\x00-\\x7F]", ""),
          "\\.", ""),
        "(?i)am", "AM")
    ).withColumn("purchase_datetime_clean",
      regexp_replace(col("purchase_datetime_clean"), "(?i)pm", "PM"))
  }

  def parseDatetime(df: DataFrame): DataFrame = {
    df.withColumn("purchase_datetime_parsed",
      to_timestamp(col("purchase_datetime_clean"), "MM/dd/yyyy hh:mm:ss a")
    ).withColumn("purchase_date",
      date_format(col("purchase_datetime_parsed"), "yyyy-MM-dd")
    )
  }

  def filterDateRange(df: DataFrame): DataFrame = {
    df.filter(col("purchase_date").between("2023-01-01", "2023-12-31"))
  }

  def aggregateByUser(df: DataFrame): DataFrame = {
    df.groupBy("user_id").agg(sum("purchase_amount").alias("total_purchase_amount"))
  }

}