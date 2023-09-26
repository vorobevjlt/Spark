package writers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.SaveMode
import writers.DataFrameWriter

object DataFrameTableWriter {

  case class WriteConfig(
    outputFile: String, 
    format: String) {
    require(Seq("orc", "parquet", "csv").contains(format), s"unsupported output format $format")
  }

}

class DataFrameTableWriter(
    spark: SparkSession, 
    config: DataFrameTableWriter.WriteConfig) extends DataFrameWriter {

  override def writeAirport(df: DataFrame): Unit = {
    require(df != null, "df session must be specified")
    df.write
      .format(config.format)
      .mode(SaveMode.Overwrite)
      .save(config.outputFile)
  }

  override def writeAirlines(df: DataFrame): Unit = {
    require(df != null, "df session must be specified")
    df.write
      .format(config.format)
      .mode(SaveMode.Overwrite)
      .save(config.outputFile)
  }

  override def writetopFlyInOneDirectionByAirport(df: DataFrame): Unit = {
    require(df != null, "df session must be specified")
    df.write
      .format(config.format)
      .mode(SaveMode.Overwrite)
      .save(config.outputFile)
  }
  override def writeTopWeekDaysByArrivalDelay(df: DataFrame): Unit = {
    require(df != null, "df session must be specified")
    df.write
      .format(config.format)
      .mode(SaveMode.Overwrite)
      .save(config.outputFile)
  }

  override def writeCountDelayReason(df: DataFrame): Unit = {
    require(df != null, "df session must be specified")
    df.write
      .format(config.format)
      .mode(SaveMode.Overwrite)
      .save(config.outputFile)
  }

  override def writegetPercentageDelayReason(df: DataFrame): Unit = {
    require(df != null, "df session must be specified")
    df.write
      .format(config.format)
      .mode(SaveMode.Overwrite)
      .save(config.outputFile)
  }
  override def writeMetaInfo(df: DataFrame): Unit = {
    require(df != null, "df session must be specified")
    df.write
      .format(config.format)
      .mode(SaveMode.Overwrite)
      .csv(config.outputFile)
  }
}

