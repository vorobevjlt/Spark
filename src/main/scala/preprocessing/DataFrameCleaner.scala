package preprocessing

import org.apache.spark.sql.DataFrame

trait DataFrameCleaner {
    def dropNoNullValues(df: DataFrame): DataFrame
    def removeNoRepetedValues(df: DataFrame): DataFrame
    def getTargetColumns(column: Seq[String])(df: DataFrame): DataFrame
}