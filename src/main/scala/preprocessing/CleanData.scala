package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import constants.Constant

class CleanData extends Constant{
        
    def hasNoNullValues(df: DataFrame): DataFrame = {
        df.na.drop()
    }

    def hasNoRepetedValues(df: DataFrame): DataFrame = {
        df.distinct()
    }

    def hasTargetColumns(column: Seq[String])(df: DataFrame): DataFrame = {
        val colNames = column.map(name => col(name))
        df.select(colNames:_*)
    }
}