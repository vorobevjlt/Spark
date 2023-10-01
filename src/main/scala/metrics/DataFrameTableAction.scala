package metrics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameTableAction extends DataFrameAction{

  def countDelayReasonFunc(column: String)(df: DataFrame): Long = {
    val condition = col(column) =!= 0

    df
    .select(col(column))
    .filter(condition)
    .count()
  }

  def sumDelayReasonFunc(column: String)(df: DataFrame): Long = {
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

//  private val hasValues = (df: DataFrame) => df.take(1).nonEmpty
//   private  val isZero = (column: String) => col(column) === 0


//   private def removeZeroValues(column: String)(df: DataFrame): DataFrame = {
//      df
//       .select(col(column))
//       .filter(!isZero(column))
//   }

//   private def getSum(column: String, df: DataFrame): Long = {
//     df
//       .agg(sum(column))
//       .first
//       .getLong(0)
//   }

//   def getDelayReasonSum(column: String)(df: DataFrame): Long = {
//     val withNoZeroValuesDF = df
//       .transform(removeZeroValues(column))

//     if (hasValues(withNoZeroValuesDF)) getSum(column, withNoZeroValuesDF)
//     else 0
//   }
// Покажите пожалуйста как это имплиментировать в джобу?