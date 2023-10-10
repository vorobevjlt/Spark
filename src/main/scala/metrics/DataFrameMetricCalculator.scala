package metrics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameMetricCalculator {
     val hasValues = (df: DataFrame) => df.take(1).nonEmpty
     val isZero = (column: String) => col(column) === 0

     def getSum(column: String, df: DataFrame): Long = {
      df
        .agg(sum(column))
        .first
        .getLong(0)
    }

  }

class DataFrameMetricCalculator extends MetricCalculator{

    override def removeZeroValues(column: String)(df: DataFrame): DataFrame = {
      df
        .select(col(column))
        .filter(!DataFrameMetricCalculator.isZero(column))      
    }

    override def getDelayReasonSum(column: String)(df: DataFrame): Long = {
      val withNoZeroValuesDF: DataFrame = df
        .transform(removeZeroValues(column))

      if (DataFrameMetricCalculator.hasValues(withNoZeroValuesDF))
        DataFrameMetricCalculator.getSum(column, withNoZeroValuesDF)
      else 0
    }
   override def countDelayReason(column: String)(df: DataFrame): Long = {
      val condition = col(column) =!= 0

      df
      .select(col(column))
      .filter(condition)
      .count()
  }

    override def getDaysDiffValue(df: DataFrame): Int = {
      df
        .withColumn("diff", datediff(col("fromIN"), col("toAR")))
        .select("diff")
        .first.getInt(0)
    }
}