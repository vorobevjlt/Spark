package readers

import org.apache.spark.sql.DataFrame

trait DataFrameReader {
  def readFlyTable(): DataFrame
  def readAirportTable(): DataFrame
  def readAirlinesTable(): DataFrame
  def readTopAirlines(): DataFrame
  def readTopAirport(): DataFrame
  def readTopFlyInOneDirectionByAirport(): DataFrame
  def readTopWeekDaysByArrivalDelay(): DataFrame
  def readCountDelayReason(): DataFrame
  def readPercentageDelayReason(): DataFrame
  def readMetaInfo(): DataFrame
}