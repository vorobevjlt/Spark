package preprocessing

import preprocessing.Cleaner
import org.apache.spark.sql.Column
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

object DataFrameCleaner {

    private def getColumns(colNames: Seq[String]): Seq[Column] = {
        colNames
            .map(name => col(name))
    }
}

class DataFrameCleaner extends Cleaner{

    override def getTargetColumns(colNames: Seq[String])(df: DataFrame): DataFrame = {
        val columns = DataFrameCleaner.getColumns(colNames)
        df.select(columns:_*)
    }

    override def dropNullValues(df: DataFrame): DataFrame = {
        df.na.drop()
    }

    override def removeDuplicates(df: DataFrame): DataFrame = {
        df.distinct()
    }
}