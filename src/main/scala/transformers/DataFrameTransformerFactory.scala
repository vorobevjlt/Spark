package transformers

import constants.Constant
import metrics.ActionFactory
import scala.SessionWrapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column 
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Row



class DataFrameTransformerFactory extends Constant with SessionWrapper{

    def hasUpdateInfo(archiveDF: DataFrame)(df: DataFrame): DataFrame = {

      val aDF = archiveDF
          .withColumnRenamed("collected", "collectedAR")
          .drop("processed")
          .withColumn("id", lit(0))

      val joinedDF = aDF.join(df, "id")

      val resultDF =
        joinedDF
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

      val flag1 = resultDF.withColumn("diff", datediff(col("fromIN"), col("toAR")))
      .select("diff")
      .first.getInt(0)

      if(flag1 >= 0) resultDF.select("collected", "processed")
      else archiveDF
    }

    def hasMetaInfoDF(df: DataFrame): DataFrame = {
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

    def hasTargetedData(target: Column)(df: DataFrame): DataFrame = {
      df
      .filter(target)
    }

    def hasCountOneGroupByKey(id: String)(df: DataFrame): DataFrame = {
      df
      .groupBy(col(id))
      .count()
    }

    def hasCountTwoGroupsByKey(df: DataFrame): DataFrame = {
      df
      .groupBy(col("airportID"),col("DestinationAiport"))
      .agg(count(lit(1)).alias("count"))
    }

    def hasSumValuesByKey(id: String, column: String)(df: DataFrame): DataFrame = {
        df
        .groupBy(col(id))
        .agg(sum(column).as(column))
    }

    def hasSumValuesByGroupKey(df: DataFrame): DataFrame = {
        df
        .groupBy(col("id"))
        .agg(
          sum("AirSystemDelayCount").as("AirSystemDelayCount"),
          sum("SecurityDelayCount").as("SecurityDelayCount"),
          sum("AirlineDelayCount").as("AirlineDelayCount"),
          sum("LateAircraftDelayCount").as("LateAircraftDelayCount"),
          sum("WeatherDelayCount").as("WeatherDelayCount"))
        .drop("id")
    }
     
    def hasOrderedDataByColumn(col: String)(df: DataFrame): DataFrame = {
      df
      .orderBy(desc(col))
      .drop("id")
    }

    def withReducedNumberOfRows(num: Int)(df: DataFrame): DataFrame = {
      df
      .limit(num)
    }

    def withTargettNameColumn(decodeDF: DataFrame, id: String)(df: DataFrame): DataFrame = {

      val bDecodeDF = broadcast(decodeDF)

      df
      .join(bDecodeDF, id)
      .select(col("name"), col("count"))
    }

    def withConcatNameCol(decodeDF: DataFrame, firstID: String, lastID: String)(df: DataFrame): DataFrame = {

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
      .select(col("name"), col("count"))
  }

    def withCountDealyReason(values: Array[Long])(df: DataFrame): DataFrame ={
      df
      .withColumn("AirSystemDelayCount", lit(values(0)))
      .withColumn("SecurityDelayCount", lit(values(1)))
      .withColumn("AirlineDelayCount", lit(values(2)))
      .withColumn("LateAircraftDelayCount", lit(values(3)))
      .withColumn("WeatherDelayCount", lit(values(4)))
      .select(col("AirSystemDelayCount"),  
              col("SecurityDelayCount"),
              col("AirlineDelayCount"),
              col("LateAircraftDelayCount"),
              col("WeatherDelayCount"))
      .limit(1)

  }
    def hasSumValuesPercent(df: DataFrame): DataFrame = {
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

    def withPercentageDealyReason(values: Array[Long])(df: DataFrame): DataFrame =  {
      val arraySum = values.reduceLeft[Long](_ + _).toDouble

      df
      .withColumn("AirSystemDelaySum", lit(values(0)))
      .withColumn("SecurityDelaySum", lit(values(1)))
      .withColumn("AirlineDelaySum", lit(values(2)))
      .withColumn("LateAircraftDelaySum", lit(values(3)))
      .withColumn("WeatherDelayCountSum", lit(values(4)))
      .withColumn("SumDelayReason", lit(arraySum))
      .select(
              col("AirSystemDelaySum"),
              col("SecurityDelaySum"),
              col("AirlineDelaySum"),
              col("LateAircraftDelaySum"),
              col("WeatherDelayCountSum"),
              col("SumDelayReason"))
      .limit(1)
  }

    def withPercentageForArchive(df: DataFrame): DataFrame = {

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
              col("AirSystemDelayPercentage"),  
              col("SecurityDelayPercentage"),
              col("AirlineDelayPercentage"),
              col("LateAircraftDelayPercentage"),
              col("WeatherDelayCountPercentage"),
              col("AirSystemDelaySum"),
              col("SecurityDelaySum"),
              col("AirlineDelaySum"),
              col("LateAircraftDelaySum"),
              col("WeatherDelayCountSum"),
              col("SumDelayReason"))
  }

    def withUpdateTable(archiveDF: DataFrame)(newDF: DataFrame): DataFrame = {
        archiveDF
            .union(newDF)
            .withColumn("id", lit(0))
    }

    def withUpdatePercentTable(archiveDF: DataFrame)(newDF: DataFrame): DataFrame = {
      val selectArchive = 
        archiveDF
        .select(
              col("AirSystemDelaySum"),
              col("SecurityDelaySum"),
              col("AirlineDelaySum"),
              col("LateAircraftDelaySum"),
              col("WeatherDelayCountSum"),
              col("SumDelayReason"))

      val selectData = 
        newDF
        .select(
              col("AirSystemDelaySum"),
              col("SecurityDelaySum"),
              col("AirlineDelaySum"),
              col("LateAircraftDelaySum"),
              col("WeatherDelayCountSum"),
              col("SumDelayReason"))

      selectArchive
            .union(selectData)
            .withColumn("id", lit(0))

    }

    def chekDate(df: DataFrame): String = {
      df.select("collected").first.getString(0)
    }
}