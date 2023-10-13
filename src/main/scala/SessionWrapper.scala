package scala

import org.apache.spark.sql.SparkSession

trait SessionWrapper {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("Flights Analyzer")
    .master("local")
    .getOrCreate()
}