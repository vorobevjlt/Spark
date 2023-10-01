package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

class DataFrameTableCleaner extends DataFrameCleaner{
        
    override def dropNoNullValues(df: DataFrame): DataFrame = {
        df.na.drop()
    }

    override def removeNoRepetedValues(df: DataFrame): DataFrame = {
        df.distinct()
    }

    override def getTargetColumns(column: Seq[String])(df: DataFrame): DataFrame = {
        val colNames = column.map(name => col(name))
        df.select(colNames:_*)
    }
}