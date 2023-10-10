package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import readers.DataFrameReader
import transformers.DataFrameTransformer
import writers.DataFrameWriter
import preprocessing.DataFrameCleaner
import metrics.DataFrameMetricCalculator
import constants.{ConstantTarget, ConstantColumns}

case class JobConfig(
    airportReaderConfig: DataFrameReader.ReadConfig,
    flightsReaderConfig: DataFrameReader.ReadConfig, 
    airlineReaderConfig: DataFrameReader.ReadConfig,
    topAirlinesConfig: DataFrameReader.ReadConfig,
    topAirportConfig: DataFrameReader.ReadConfig,
    topFlyInOneDirectionByAirportConfig: DataFrameReader.ReadConfig,
    topWeekDaysByArrivalDelayConfig: DataFrameReader.ReadConfig,
    countDelayReasonConfig: DataFrameReader.ReadConfig,
    percentageDelayReasonConfig: DataFrameReader.ReadConfig,
    metaInfoConfig: DataFrameReader.ReadConfig,
    writeAirportConfig: DataFrameWriter.WriteConfig,
    writeAirlinesConfig: DataFrameWriter.WriteConfig,
    writetopFlyInOneDirectionByAirportConfig: DataFrameWriter.WriteConfig,
    writeTopWeekDaysByArrivalDelayConfig: DataFrameWriter.WriteConfig,
    writeCountDelayReasonConfig: DataFrameWriter.WriteConfig,
    writegetPercentageDelayReasonConfig: DataFrameWriter.WriteConfig,
    writeMetaInfoConfig: DataFrameWriter.WriteConfig
    )

class FlyghtAnalysisJob (
    spark: SparkSession, 
    config: JobConfig ) extends Job{

   override def run(): Unit = {

      val getAirport = {
        new DataFrameReader(
          spark, 
          config.airportReaderConfig)
      }

      val getFlights = {
        new DataFrameReader(
          spark, 
          config.flightsReaderConfig)
      }

      val getAirline = {
        new DataFrameReader(
          spark, 
          config.airlineReaderConfig)
      }

      val getArchiveTopAirlines = {
        new DataFrameReader(
          spark, 
          config.topAirlinesConfig)
      }

      val getArchiveTopAirport = {
        new DataFrameReader(
          spark, 
          config.topAirportConfig)
      }

      val getArchiveFlyInOneDirectionByAirport = {
        new DataFrameReader(
          spark, 
          config.topFlyInOneDirectionByAirportConfig)
      }

      val getArchiveTopWeekDays = {
        new DataFrameReader(
          spark, 
          config.topWeekDaysByArrivalDelayConfig)
      }

      val getArchiveCountDelayReason = {
        new DataFrameReader(
          spark, 
          config.countDelayReasonConfig)
      }

      val getArchivePercentageDelayReason = {
        new DataFrameReader(
          spark, 
          config.percentageDelayReasonConfig)
      }

      val getArchiveMetaInfo = {
        new DataFrameReader(
          spark, 
          config.metaInfoConfig)
      }

      val writeTopAirport = {
        new DataFrameWriter(
          spark, 
          config.writeAirportConfig)
      }

      val writeTopAirlines = {
        new DataFrameWriter(
          spark, 
          config.writeAirlinesConfig)
      }

      val writeTopFlyInOneDirectionByAirport = {
        new DataFrameWriter(
          spark, 
          config.writetopFlyInOneDirectionByAirportConfig)
      }
      val writeTopWeekDaysByArrivalDelay = {
        new DataFrameWriter(
          spark, 
          config.writeTopWeekDaysByArrivalDelayConfig)
      }
      val writeCountDelayReason = {
        new DataFrameWriter(
          spark, 
          config.writeCountDelayReasonConfig)
      }
      val writeGetPercentageDelayReason = {
        new DataFrameWriter(
          spark, 
          config.writegetPercentageDelayReasonConfig)
      }
      val writeMetaInfo = {
        new DataFrameWriter(
          spark, 
          config.writeMetaInfoConfig)
      }

      def isTransformer(): DataFrameTransformer = {
        new DataFrameTransformer()
      }

      def isAction(): DataFrameMetricCalculator = {
        new DataFrameMetricCalculator()
      }

      def isPreprocess(): DataFrameCleaner = {
        new DataFrameCleaner()
      }
      
      def getTop10ByKey(
        condition: Column, 
        id: String, 
        decode: DataFrame
        )(df: DataFrame): DataFrame = {
        df
          .transform(isTransformer()
            .hasTargetedData(condition))
          .transform(isTransformer()
            .countOneGroupByKey(id))
          .transform(isTransformer()
            .orderedDataByColumn("count"))
          .transform(isTransformer()
            .withReducedNumberOfRows(10))
          .transform(isTransformer()
            .joinTables(decode, id))
          .transform(isPreprocess().
            getTargetColumns(Seq[String]("name", "count")))
      }
      
      def getGroupAndOrder(df: DataFrame): DataFrame = {
        df
          .transform(isTransformer()
            .sumValuesByKey("name", "count"))
          .transform(isTransformer()
            .orderedDataByColumn("count"))
      }

      val flightsDF = getFlights.read()

      val ArchiveMetaInfoDF = getArchiveMetaInfo.read()

      val newMetaInfoDF = flightsDF
        .transform(isTransformer()
            .getMetaInfoDF)
            
      val archiveMetaInfoDF = ArchiveMetaInfoDF
          .transform(isTransformer()
            .withIdColumnForJoin)

      val withJoinedMetaInfoDF = newMetaInfoDF
        .transform(isTransformer()
            .joinTables(archiveMetaInfoDF, "id"))
        .transform(isTransformer()
            .withConcatColumns)
      
      val getDiffDays = isAction()
            .getDaysDiffValue(withJoinedMetaInfoDF)

      val metaInfoDF = withJoinedMetaInfoDF
        .transform(isTransformer()
            .getResultMetaInfo(getDiffDays, ArchiveMetaInfoDF))

      val incomeDF = 
        isTransformer().chekDate(metaInfoDF)
      val archiveDF = 
        isTransformer().chekDate(ArchiveMetaInfoDF)
      
      if(incomeDF == archiveDF) println("Wrong data")
        else { 
        
          val airportDF = getAirport.read()

          val airlineDF = getAirline.read()

          val ArchiveTopAirlinesDF = getArchiveTopAirlines.read()

          val ArchiveTopAirportDF = getArchiveTopAirport.read()

          val ArchiveTopFlyDF = getArchiveFlyInOneDirectionByAirport.read()
          
          val ArchiveTopWeekDaysDF = getArchiveTopWeekDays.read()
          
          val ArchiveCountDelayDF = getArchiveCountDelayReason.read()

          val ArchivePercentageDelayDF = getArchivePercentageDelayReason.read()

          val CountAirSystemDelay = 
            isAction()
              .countDelayReason("AirSystemDelay")(flightsDF)

          val CountSecurityDelay = 
            isAction()
              .countDelayReason("SecurityDelay")(flightsDF)

          val CountAirlineDelay =
            isAction()
              .countDelayReason("AirlineDelay")(flightsDF)                                                 

          val CountLateAircraftDelay = 
            isAction()
              .countDelayReason("LateAircraftDelay")(flightsDF)

          val CountWeatherDelay = 
            isAction()
              .countDelayReason("WeatherDelay")(flightsDF)


          val sumAirSystemDelay = 
            isAction()
              .getDelayReasonSum("AirSystemDelay")(flightsDF)

          val sumSecurityDelay = 
            isAction()
              .getDelayReasonSum("SecurityDelay")(flightsDF)

          val sumAirlineDelay = 
            isAction()
              .getDelayReasonSum("AirlineDelay")(flightsDF)                                                 

          val sumLateAircraftDelay = 
            isAction()
              .getDelayReasonSum("LateAircraftDelay")(flightsDF)

          val sumWeatherDelay = 
            isAction()
              .getDelayReasonSum("WeatherDelay")(flightsDF)


          val countDelay: Array[Long] = 
              Array(CountAirSystemDelay, 
                    CountSecurityDelay, 
                    CountAirlineDelay, 
                    CountLateAircraftDelay, 
                    CountWeatherDelay)

          val sumDelay: Array[Long] = 
              Array(sumAirSystemDelay, 
                    sumSecurityDelay, 
                    sumAirlineDelay, 
                    sumLateAircraftDelay, 
                    sumWeatherDelay)  


          val topAirportDF = flightsDF
            .transform(isPreprocess()
              .getTargetColumns(ConstantColumns.HasAirportColumns))
            .transform(isPreprocess()
              .dropNullValues)
            .transform(getTop10ByKey(ConstantTarget.HasTargetCancelFly, 
                                    "airportID", 
                                    airportDF))
            .transform(isTransformer()
              .withUpdateTable(ArchiveTopAirportDF))
            .transform(getGroupAndOrder)

          val topAirlinesDF = flightsDF
            .transform(isPreprocess()
              .getTargetColumns(ConstantColumns.HasAirlineColumns))
            .transform(isPreprocess()
              .dropNullValues)
            .transform(getTop10ByKey(ConstantTarget.HasTargetDepartureDelay, 
                                    "airlineID", 
                                    airlineDF))
            .transform(isTransformer()
              .withUpdateTable(ArchiveTopAirlinesDF))
            .transform(getGroupAndOrder)

          val topFlyInOneDirectionByAirportDF = flightsDF
            .transform(isPreprocess()
              .getTargetColumns(ConstantColumns.HasDestinationColumns))
            .transform(isPreprocess()
              .dropNullValues)
            .transform(isTransformer()
              .countTwoGroupsByKey)
            .transform(isTransformer()
              .orderedDataByColumn("count"))
            .transform(isTransformer()
              .withReducedNumberOfRows(10))
            .transform(isTransformer()
              .withConcatNameCol(airportDF, 
                                "airportID", 
                                "DestinationAiport"))
            .transform(isPreprocess().
              getTargetColumns(Seq[String]("name", "count")))
            .transform(isTransformer()
              .withUpdateTable(ArchiveTopFlyDF))
            .transform(getGroupAndOrder)

          val topWeekDaysByArrivalDelayDF = flightsDF
            .transform(isPreprocess()
              .getTargetColumns(ConstantColumns.HasDayWeekColumns))
            .transform(isTransformer()
              .countOneGroupByKey("DayOfWeek"))
            .transform(isTransformer()
              .orderedDataByColumn("count"))
            .transform(isTransformer()
              .withUpdateTable(ArchiveTopWeekDaysDF))
            .transform(isTransformer()
              .sumValuesByKey("DayOfWeek", "count"))

          val delayByReasonDF = flightsDF
            .transform(isPreprocess()
              .getTargetColumns(ConstantColumns.HasDelayColumns))
            .transform(isTransformer()
              .hasTargetedData(ConstantTarget.HasTargetDelay))

          val countDelayReasonDF = delayByReasonDF
            .transform(isTransformer()
              .withCountDealyReason(countDelay))
            .transform(isTransformer()
              .withUpdateTable(ArchiveCountDelayDF))
            .transform(isTransformer()
              .sumValuesByGroupKey)
          
          val getPercentageDelayReasonDF = delayByReasonDF
            .transform(isTransformer()
              .withPercentageDealyReason(sumDelay)) 
            .transform(isTransformer()
              .withUpdatePercentTable(ArchivePercentageDelayDF))
            .transform(isTransformer().sumValuesPercent)
            .transform(isTransformer().withPercentageForArchive)

          writeGetPercentageDelayReason.write(getPercentageDelayReasonDF)
          writeTopAirlines.write(topAirlinesDF)
          writeTopAirport.write(topAirportDF)
          writeTopFlyInOneDirectionByAirport.write(topFlyInOneDirectionByAirportDF)
          writeTopWeekDaysByArrivalDelay.write(topWeekDaysByArrivalDelayDF)
          writeCountDelayReason.write(countDelayReasonDF)
          writeMetaInfo.write(metaInfoDF)}
  }
}
    
