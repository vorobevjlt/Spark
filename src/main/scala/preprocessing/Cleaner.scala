package preprocessing

import org.apache.spark.sql.DataFrame

trait Cleaner {
    def dropNullValues(df: DataFrame): DataFrame
    def removeDuplicates(df: DataFrame): DataFrame
    def getTargetColumns(colNames: Seq[String])(df: DataFrame): DataFrame
}