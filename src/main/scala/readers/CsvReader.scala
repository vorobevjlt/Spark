package readers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import readers.DataFrameReader

object CsvReader {
  case class ReadConfig(
    file: String,
    schema: StructType,
    hasHeader: Boolean,
    separator: Char)
}

class CsvReader(
  spark: SparkSession, 
  config: CsvReader.ReadConfig) extends DataFrameReader {

  override def readAirportTable(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .csv(config.file)
  }

  override def readFlyTable(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .csv(config.file)
  }

  override def readAirlinesTable(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .csv(config.file)
  }

  override def readTopAirlines(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .parquet(config.file)
  }
  
  override def readTopAirport(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .parquet(config.file)
  }
  
  override def readTopFlyInOneDirectionByAirport(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .parquet(config.file)
  }
  
  override def readTopWeekDaysByArrivalDelay(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .parquet(config.file)
  }
  
  override def readCountDelayReason(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .parquet(config.file)
  }
  
  override def readPercentageDelayReason(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .parquet(config.file)
  }

  override def readMetaInfo(): DataFrame = {
    spark.read
      .option("header", config.hasHeader.toString.toLowerCase)
      .option("sep", config.separator.toString)
      .schema(config.schema)
      .csv(config.file)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.WARN)
}