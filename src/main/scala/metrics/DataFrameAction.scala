package metrics

import org.apache.spark.sql.DataFrame

trait DataFrameAction {
    def countDelayReasonFunc(column: String)(df: DataFrame): Long
    def sumDelayReasonFunc(column: String)(df: DataFrame): Long
}