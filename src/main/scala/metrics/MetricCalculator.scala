package metrics

import org.apache.spark.sql.DataFrame

trait MetricCalculator {
    def countDelayReasonFunc(column: String)(df: DataFrame): Long
    def sumDelayReasonFunc(column: String)(df: DataFrame): Long
}