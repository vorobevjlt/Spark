package writers

import org.apache.spark.sql.DataFrame

trait DataFrameWriter {
  def write(df: DataFrame): Unit
}
