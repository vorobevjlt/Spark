package writers

import org.apache.spark.sql.DataFrame

trait DataFrameWriter {
  def writeAirport(df: DataFrame): Unit
  def writeAirlines(df: DataFrame): Unit
  def writetopFlyInOneDirectionByAirport(df: DataFrame): Unit
  def writeTopWeekDaysByArrivalDelay(df: DataFrame): Unit
  def writeCountDelayReason(df: DataFrame): Unit
  def writegetPercentageDelayReason(df: DataFrame): Unit
  def writeMetaInfo(df: DataFrame): Unit
}
