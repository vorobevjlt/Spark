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
    flyReaderConfig: DataFrameReader.ReadConfig, 
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

      val getfly = {
        new DataFrameReader(
          spark, 
          config.flyReaderConfig)
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

      val flyDF = getfly.read()

      val ArchiveMetaInfoDF = getArchiveMetaInfo.read()

      val withCollectedProcessedColDF = flyDF
        .transform(isTransformer()
            .getMetaInfoDF)
            
      val withIdColArchiveDF = ArchiveMetaInfoDF
          .transform(isTransformer()
            .withIdColumnForJoin)

      val metaInfoDF = withCollectedProcessedColDF
        .transform(isTransformer()
            .joinTables(withIdColArchiveDF, "id"))
        .transform(isTransformer()
            .withConcatColumns)
        .transform(isTransformer()
            .checkTableByCondition(ArchiveMetaInfoDF))

      val incomeDF = 
        isTransformer().chekDate(metaInfoDF)
      val archiveDF = 
        isTransformer().chekDate(ArchiveMetaInfoDF)
      
      if(1 == 1) println("Wrong data")
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
              .countDelayReasonFunc("AirSystemDelay")(flyDF)

          val CountSecurityDelay = 
            isAction()
              .countDelayReasonFunc("SecurityDelay")(flyDF)

          val CountAirlineDelay =
            isAction()
              .countDelayReasonFunc("AirlineDelay")(flyDF)                                                 

          val CountLateAircraftDelay = 
            isAction()
              .countDelayReasonFunc("LateAircraftDelay")(flyDF)

          val CountWeatherDelay = 
            isAction()
              .countDelayReasonFunc("WeatherDelay")(flyDF)


          val sumAirSystemDelay = 
            isAction()
              .sumDelayReasonFunc("AirSystemDelay")(flyDF)

          val sumSecurityDelay = 
            isAction()
              .sumDelayReasonFunc("SecurityDelay")(flyDF)

          val sumAirlineDelay = 
            isAction()
              .sumDelayReasonFunc("AirlineDelay")(flyDF)                                                 

          val sumLateAircraftDelay = 
            isAction()
              .sumDelayReasonFunc("LateAircraftDelay")(flyDF)

          val sumWeatherDelay = 
            isAction()
              .sumDelayReasonFunc("WeatherDelay")(flyDF)


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


          val topAirportDF = flyDF
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

          val topAirlinesDF = flyDF
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

          val topFlyInOneDirectionByAirportDF = flyDF
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

          val topWeekDaysByArrivalDelayDF = flyDF
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

          val delayByReasonDF = flyDF
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
    
