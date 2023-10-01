package readers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import readers.DataFrameReader

object DataFrameTableReader {
  case class ReadConfig(
    file: String,
    schema: StructType,
    hasHeader: Boolean,
    separator: Char)
}

class DataFrameTableReader(
  spark: SparkSession, 
  config: DataFrameTableReader.ReadConfig) extends DataFrameReader {

  override def read(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .csv(config.file)    
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.WARN)
}