package metrics

import org.apache.spark.sql.DataFrame

trait MetricCalculator {
    def getDelayReasonSum(column: String)(df: DataFrame): Long
    def countDelayReason(column: String)(df: DataFrame): Long
    def getDaysDiffValue(df: DataFrame): Int
    def removeZeroValues(column: String)(df: DataFrame): DataFrame 
}