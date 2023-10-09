package transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column

trait Transformer {
    def getMetaInfoDF(df: DataFrame): DataFrame
    def hasTargetedData(target: Column)(df: DataFrame): DataFrame
    def countOneGroupByKey(id: String)(df: DataFrame): DataFrame
    def countTwoGroupsByKey(df: DataFrame): DataFrame
    def sumValuesByKey(id: String, column: String)(df: DataFrame): DataFrame
    def sumValuesByGroupKey(df: DataFrame): DataFrame
    def orderedDataByColumn(col: String)(df: DataFrame): DataFrame
    def withReducedNumberOfRows(num: Int)(df: DataFrame): DataFrame
    def joinTables(jdf: DataFrame, id: String)(df: DataFrame): DataFrame
    def withConcatNameCol(decodeDF: DataFrame, firstID: String, lastID: String)(df: DataFrame): DataFrame
    def withCountDealyReason(values: Array[Long])(df: DataFrame): DataFrame
    def sumValuesPercent(df: DataFrame): DataFrame
    def withPercentageDealyReason(values: Array[Long])(df: DataFrame): DataFrame
    def withPercentageForArchive(df: DataFrame): DataFrame
    def withUpdateTable(archiveDF: DataFrame)(df: DataFrame): DataFrame
    def withUpdatePercentTable(archiveDF: DataFrame)(df: DataFrame): DataFrame
    def chekDate(df: DataFrame): String
    def withIdColumnForJoin(df: DataFrame): DataFrame
    def withConcatColumns(df: DataFrame): DataFrame
    def checkTableByCondition(archiveDF: DataFrame)(df: DataFrame): DataFrame
    def extractColumnsByTarget(column: String)(df: DataFrame): DataFrame
}