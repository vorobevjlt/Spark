package metrics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.{DataFrame, SparkSession}

class ActionFactory{

  def hasCountDelayReason(column: String)(df: DataFrame): Long = {
    val condition = col(column) =!= 0

    df
    .select(col(column))
    .filter(condition)
    .count()
  }

  def hasSumDelayReason(column: String)(df: DataFrame): Long = {
    val condition = col(column) =!= 0

    val withNoZeroValuesDF = df
      .select(col(column))
      .filter(condition)
    if(withNoZeroValuesDF.take(1).nonEmpty)
       withNoZeroValuesDF
      .agg(sum(column))
      .first.getLong(0)
    else 0
  }
    
}
