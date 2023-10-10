package transformers

import constants.{ConstantTarget, ConstantColumns}
import metrics.DataFrameMetricCalculator
import scala.SessionWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Row

object DFColumn extends DFColumn {
  implicit def columnToString(col: DFColumn.Value): String = col.toString
}

trait DFColumn extends Enumeration {
  val airportID,
      DestinationAiport,
      id,
      AirSystemDelaySum,
      SecurityDelaySum,
      AirlineDelaySum,
      LateAircraftDelaySum,
      WeatherDelayCountSum,
      SumDelayReason,
      AirSystemDelayPercentage,
      SecurityDelayPercentage,
      AirlineDelayPercentage,
      LateAircraftDelayPercentage,
      WeatherDelayCountPercentage,
      AirSystemDelayCount, 
      SecurityDelayCount,
      AirlineDelayCount,
      LateAircraftDelayCount,  
      WeatherDelayCount = Value

}

class DataFrameTransformer extends Transformer{

    override def extractColumnsByTarget(column: String)(df: DataFrame): DataFrame = {
      val condition = col(column) =!= 0

      df
        .select(col(column))
        .filter(condition)
  }

    override def hasTargetedData(target: Column)(df: DataFrame): DataFrame = {
      df
      .filter(target)
    }

    override def countOneGroupByKey(id: String)(df: DataFrame): DataFrame = {
      df
      .groupBy(col(id))
      .count()
    }

    override def countTwoGroupsByKey(df: DataFrame): DataFrame = {
      df
      .groupBy(col(DFColumn.airportID),col(DFColumn.DestinationAiport))
      .agg(count(lit(1)).alias("count"))
    }

    override def sumValuesByKey(id: String, column: String)(df: DataFrame): DataFrame = {
        df
        .groupBy(col(id))
        .agg(sum(column).as(column))
    }

    override def sumValuesByGroupKey(df: DataFrame): DataFrame = {
        df
        .groupBy(col(DFColumn.id))
        .agg(
          sum("AirSystemDelayCount").as("AirSystemDelayCount"),
          sum("SecurityDelayCount").as("SecurityDelayCount"),
          sum("AirlineDelayCount").as("AirlineDelayCount"),
          sum("LateAircraftDelayCount").as("LateAircraftDelayCount"),
          sum("WeatherDelayCount").as("WeatherDelayCount"))
        .drop("id")
    }
     
    override def orderedDataByColumn(col: String)(df: DataFrame): DataFrame = {
      df
      .orderBy(desc(col))
      .drop("id")
    }

    override def withReducedNumberOfRows(num: Int)(df: DataFrame): DataFrame = {
      df
      .limit(num)
    }

    override def withConcatNameCol(decodeDF: DataFrame, firstID: String, lastID: String)(df: DataFrame): DataFrame = {

      val firstIdDecodeDF = decodeDF
        .withColumnRenamed("airportID", firstID)
        .withColumnRenamed("name", "from")

      val lastIdDecodeDF = decodeDF
        .withColumnRenamed("airportID", lastID)
        .withColumnRenamed("name", "to")

      val bFirstIdDecodeDF = broadcast(firstIdDecodeDF)

      val bLastIdDecodeDF = broadcast(lastIdDecodeDF)
        
      df
      .join(bFirstIdDecodeDF, firstID)
      .join(bLastIdDecodeDF, lastID)
      .withColumn("name", 
                  concat(
                    col("from"), 
                    lit(" - "), 
                    col("to")))
  }

    override def withCountDealyReason(values: Array[Long])(df: DataFrame): DataFrame ={
      df
      .withColumn("AirSystemDelayCount", lit(values(0)))
      .withColumn("SecurityDelayCount", lit(values(1)))
      .withColumn("AirlineDelayCount", lit(values(2)))
      .withColumn("LateAircraftDelayCount", lit(values(3)))
      .withColumn("WeatherDelayCount", lit(values(4)))
      .select(col(DFColumn.AirSystemDelayCount),  
              col(DFColumn.SecurityDelayCount),
              col(DFColumn.AirlineDelayCount),
              col(DFColumn.LateAircraftDelayCount),
              col(DFColumn.WeatherDelayCount))
      .limit(1)

  }
    override def sumValuesPercent(df: DataFrame): DataFrame = {
      df
      .groupBy(col("id"))
      .agg(
        sum("AirSystemDelaySum").as("AirSystemDelaySum"),
        sum("SecurityDelaySum").as("SecurityDelaySum"),
        sum("AirlineDelaySum").as("AirlineDelaySum"),
        sum("LateAircraftDelaySum").as("LateAircraftDelaySum"),
        sum("WeatherDelayCountSum").as("WeatherDelayCountSum"),
        sum("SumDelayReason").as("SumDelayReason"))
      .drop("id")
  }

    override def withPercentageDealyReason(values: Array[Long])(df: DataFrame): DataFrame =  {
      val arraySum = values.reduceLeft[Long](_ + _).toDouble

      df
      .withColumn("AirSystemDelaySum", lit(values(0)))
      .withColumn("SecurityDelaySum", lit(values(1)))
      .withColumn("AirlineDelaySum", lit(values(2)))
      .withColumn("LateAircraftDelaySum", lit(values(3)))
      .withColumn("WeatherDelayCountSum", lit(values(4)))
      .withColumn("SumDelayReason", lit(arraySum))
      .select(
              col(DFColumn.AirSystemDelaySum),
              col(DFColumn.SecurityDelaySum),
              col(DFColumn.AirlineDelaySum),
              col(DFColumn.LateAircraftDelaySum),
              col(DFColumn.WeatherDelayCountSum),
              col(DFColumn.SumDelayReason))
      .limit(1)
  }

    override def withPercentageForArchive(df: DataFrame): DataFrame = {

      def getValue(colum: String): Long = {
        val column = df.select(col(colum))
         if(column.take(1).nonEmpty)column.first.getLong(0)
        else 0
      }

      val fAirSystemDelayPercentage = getValue("AirSystemDelaySum")
      val fSecurityDelayPercentage = getValue("SecurityDelaySum")
      val fAirlineDelayPercentage = getValue("AirlineDelaySum")
      val fLateAircraftDelayPercentage = getValue("LateAircraftDelaySum")
      val fWeatherDelayCountPercentage = getValue("WeatherDelayCountSum")
      val fSumDelayReason = df.select(col("SumDelayReason")).first.getDouble(0)

      val AirSystemDelayPercentage = (fAirSystemDelayPercentage/fSumDelayReason)*100
      val SecurityDelayPercentage = (fSecurityDelayPercentage/fSumDelayReason)*100
      val AirlineDelayPercentage = (fAirlineDelayPercentage/fSumDelayReason)*100
      val LateAircraftDelayPercentage = (fLateAircraftDelayPercentage/fSumDelayReason)*100
      val WeatherDelayCountPercentage = (fWeatherDelayCountPercentage/fSumDelayReason)*100

      df
      .withColumn("AirSystemDelayPercentage", lit(AirSystemDelayPercentage))
      .withColumn("SecurityDelayPercentage", lit(SecurityDelayPercentage))
      .withColumn("AirlineDelayPercentage", lit(AirlineDelayPercentage))
      .withColumn("LateAircraftDelayPercentage", lit(LateAircraftDelayPercentage))
      .withColumn("WeatherDelayCountPercentage", lit(WeatherDelayCountPercentage))
      .select(
              col(DFColumn.AirSystemDelayPercentage),  
              col(DFColumn.SecurityDelayPercentage),
              col(DFColumn.AirlineDelayPercentage),
              col(DFColumn.LateAircraftDelayPercentage),
              col(DFColumn.WeatherDelayCountPercentage),
              col(DFColumn.AirSystemDelaySum),
              col(DFColumn.SecurityDelaySum),
              col(DFColumn.AirlineDelaySum),
              col(DFColumn.LateAircraftDelaySum),
              col(DFColumn.WeatherDelayCountSum),
              col(DFColumn.SumDelayReason))
    }

    override def withUpdateTable(archiveDF: DataFrame)(newDF: DataFrame): DataFrame = {
        archiveDF
            .union(newDF)
            .withColumn("id", lit(0))
    }

    override def withUpdatePercentTable(archiveDF: DataFrame)(newDF: DataFrame): DataFrame = {
      val selectArchive = 
        archiveDF
        .select(
              col(DFColumn.AirSystemDelaySum),
              col(DFColumn.SecurityDelaySum),
              col(DFColumn.AirlineDelaySum),
              col(DFColumn.LateAircraftDelaySum),
              col(DFColumn.WeatherDelayCountSum),
              col(DFColumn.SumDelayReason))

      val selectData = 
        newDF
        .select(
              col(DFColumn.AirSystemDelaySum),
              col(DFColumn.SecurityDelaySum),
              col(DFColumn.AirlineDelaySum),
              col(DFColumn.LateAircraftDelaySum),
              col(DFColumn.WeatherDelayCountSum),
              col(DFColumn.SumDelayReason))

      selectArchive
            .union(selectData)
            .withColumn("id", lit(0))
    }

    override def chekDate(df: DataFrame): String = {
      df.select("collected").first.getString(0)
    }

    override def getMetaInfoDF(df: DataFrame): DataFrame = {
      df
      .withColumn("date", 
            to_date(concat(
                    col("YEAR"),
                    lit("-"), 
                    col("MONTH"),
                    lit("-"),
                    col("DAY"))))
      .withColumn("id", lit(0))
      .select(col("date"), col("id"))
      .groupBy(col("id"))
      .agg(min("date").as("from"), max("date").as("to"))
      .withColumn("collected", 
                concat(
                      lit("from "),
                      col("from"),
                      lit(" to "), 
                      col("to")))
      .withColumn("processed", current_date())
      }

    override def withIdColumnForJoin(df: DataFrame): DataFrame = {
      df
        .withColumnRenamed("collected", "collectedAR")
        .drop("processed")
        .withColumn("id", lit(0))
    }

    override def withConcatColumns(df: DataFrame): DataFrame = {
        df
        .withColumn("fromAR", split(col("collectedAR"), " ")(1))
        .withColumn("toAR", split(col("collectedAR"), " ")(3))
        .withColumn("fromIN", split(col("collected"), " ")(1))
        .withColumn("toIN", split(col("collected"), " ")(3))
        .withColumn("collected", 
          concat
          (
            lit("from "),
            col("fromAR"),
            lit(" to "),
            col("toIN"))
          )
    }

    override def getResultMetaInfo(dayCount: Int, archiveDF: DataFrame)(df: DataFrame): DataFrame = {
      if(dayCount >= 0) df.select("collected", "processed")
      else archiveDF
    }    

    override def joinTables(jdf: DataFrame, id: String)(df: DataFrame): DataFrame = {
      val bDecodeDF = broadcast(jdf)

      df
      .join(bDecodeDF, id)
    }

}