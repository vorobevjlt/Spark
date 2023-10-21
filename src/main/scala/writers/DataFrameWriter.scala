package writers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.SaveMode
import writers.Writer

object DataFrameWriter {

  case class WriteConfig(
    outputFile: String, 
    format: String) {
    require(Seq("orc", "parquet", "csv").contains(format), s"unsupported output format $format")
  }

}

class DataFrameWriter(
    config: DataFrameWriter.WriteConfig) extends Writer {

  override def write(df: DataFrame): Unit = {
    require(df != null, "df session must be specified")
    df.write
      .format(config.format)
      .mode(SaveMode.Overwrite)
      .save(config.outputFile)
  }
}

