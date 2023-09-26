package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Column 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import readers.CsvReader
import readers.DataFrameReader
import transformers.DataFrameTransformerFactory
import writers.DataFrameTableWriter
import writers.DataFrameWriter
import preprocessing.CleanData
import metrics.ActionFactory
import constants.Constant

case class JobConfig(
    airportReaderConfig: CsvReader.ReadConfig,
    flyReaderConfig: CsvReader.ReadConfig, 
    airlineReaderConfig: CsvReader.ReadConfig,
    topAirlinesConfig: CsvReader.ReadConfig,
    topAirportConfig: CsvReader.ReadConfig,
    topFlyInOneDirectionByAirportConfig: CsvReader.ReadConfig,
    topWeekDaysByArrivalDelayConfig: CsvReader.ReadConfig,
    countDelayReasonConfig: CsvReader.ReadConfig,
    percentageDelayReasonConfig: CsvReader.ReadConfig,
    metaInfoConfig: CsvReader.ReadConfig,
    writeAirportConfig: DataFrameTableWriter.WriteConfig,
    writeAirlinesConfig: DataFrameTableWriter.WriteConfig,
    writetopFlyInOneDirectionByAirportConfig: DataFrameTableWriter.WriteConfig,
    writeTopWeekDaysByArrivalDelayConfig: DataFrameTableWriter.WriteConfig,
    writeCountDelayReasonConfig: DataFrameTableWriter.WriteConfig,
    writegetPercentageDelayReasonConfig: DataFrameTableWriter.WriteConfig,
    writeMetaInfoConfig: DataFrameTableWriter.WriteConfig
    )

class Job (
    spark: SparkSession, 
    config: JobConfig) extends Constant{

    def run(): Unit = {

      def getAirportTable(): DataFrameReader = {
        new CsvReader(
          spark, 
          config.airportReaderConfig)
      }

      def getflyTable(): DataFrameReader = {
        new CsvReader(
          spark, 
          config.flyReaderConfig)
      }

      def getAirlineTable(): DataFrameReader = {
        new CsvReader(
          spark, 
          config.airlineReaderConfig)
      }

      def getArchiveTopAirlines(): DataFrameReader = {
        new CsvReader(
          spark, 
          config.topAirlinesConfig)
      }

      def getArchiveTopAirport(): DataFrameReader = {
        new CsvReader(
          spark, 
          config.topAirportConfig)
      }

      def getArchiveFlyInOneDirectionByAirport(): DataFrameReader = {
        new CsvReader(
          spark, 
          config.topFlyInOneDirectionByAirportConfig)
      }

      def getArchiveTopWeekDays(): DataFrameReader = {
        new CsvReader(
          spark, 
          config.topWeekDaysByArrivalDelayConfig)
      }

      def getArchiveCountDelayReason(): DataFrameReader = {
        new CsvReader(
          spark, 
          config.countDelayReasonConfig)
      }

      def getArchivePercentageDelayReason(): DataFrameReader = {
        new CsvReader(
          spark, 
          config.percentageDelayReasonConfig)
      }

      def getArchiveMetaInfo(): DataFrameReader = {
        new CsvReader(
          spark, 
          config.metaInfoConfig)
      }

      def isWriteTopAirport(): DataFrameWriter = {
        new DataFrameTableWriter(
          spark, 
          config.writeAirportConfig)
      }

      def isWriteTopAirlines(): DataFrameWriter = {
        new DataFrameTableWriter(
          spark, 
          config.writeAirlinesConfig)
      }

      def isWriteTopFlyInOneDirectionByAirport(): DataFrameWriter = {
        new DataFrameTableWriter(
          spark, 
          config.writetopFlyInOneDirectionByAirportConfig)
      }
      def isWriteTopWeekDaysByArrivalDelay(): DataFrameWriter = {
        new DataFrameTableWriter(
          spark, 
          config.writeTopWeekDaysByArrivalDelayConfig)
      }
      def isWriteCountDelayReason(): DataFrameWriter = {
        new DataFrameTableWriter(
          spark, 
          config.writeCountDelayReasonConfig)
      }
      def isWriteGetPercentageDelayReason(): DataFrameWriter = {
        new DataFrameTableWriter(
          spark, 
          config.writegetPercentageDelayReasonConfig)
      }
      def isWriteMetaInfo(): DataFrameWriter = {
        new DataFrameTableWriter(
          spark, 
          config.writeMetaInfoConfig)
      }

      def isTransformer(): DataFrameTransformerFactory = {
        new DataFrameTransformerFactory()
      }

      def isAction(): ActionFactory = {
        new ActionFactory()
      }

      def isPreprocess(): CleanData = {
        new CleanData()
      }
      
      def getTop10ByKey(
        condition: Column, 
        id: String, decode: DataFrame)
        (df: DataFrame): DataFrame = {
        df
          .transform(isTransformer()
            .hasTargetedData(condition))
          .transform(isTransformer()
            .hasCountOneGroupByKey(id))
          .transform(isTransformer()
            .hasOrderedDataByColumn("count"))
          .transform(isTransformer()
            .withReducedNumberOfRows(10))
          .transform(isTransformer()
            .withTargettNameColumn(decode, id))
      }

      def getGroupAndOrder(df: DataFrame): DataFrame = {
        df
          .transform(isTransformer()
            .hasSumValuesByKey("name", "count"))
          .transform(isTransformer()
            .hasOrderedDataByColumn("count"))
      }

      val flyDF = 
        getflyTable()
        .readFlyTable()

      val ArchiveMetaInfoDF = 
        getArchiveMetaInfo()
        .readMetaInfo()

      val metaInfoDF = 
        flyDF
          .transform(isTransformer()
            .hasMetaInfoDF)
          .transform(isTransformer()
            .hasUpdateInfo(ArchiveMetaInfoDF))

      val incomeDF = 
        isTransformer().chekDate(metaInfoDF)
      val archiveDF = 
        isTransformer().chekDate(ArchiveMetaInfoDF)
      
      if(incomeDF == archiveDF) println("Wrong data")
        else { 
        
          val airportDF = 
            getAirportTable()
              .readAirportTable()

          val airlineDF = 
            getAirlineTable()
              .readAirlinesTable()

          val ArchiveTopAirlinesDF = 
            getArchiveTopAirlines()
              .readTopAirlines()

          val ArchiveTopAirportDF = 
            getArchiveTopAirport()
              .readTopAirport()

          val ArchiveTopFlyDF = 
            getArchiveFlyInOneDirectionByAirport()
              .readTopFlyInOneDirectionByAirport()
          
          val ArchiveTopWeekDaysDF = 
            getArchiveTopWeekDays()
              .readTopWeekDaysByArrivalDelay()
          
          val ArchiveCountDelayDF = 
            getArchiveCountDelayReason()
              .readCountDelayReason()

          val ArchivePercentageDelayDF = 
            getArchivePercentageDelayReason()
              .readPercentageDelayReason()

          val CountAirSystemDelay = 
            isAction()
              .hasCountDelayReason("AirSystemDelay")(flyDF)

          val CountSecurityDelay = 
            isAction()
              .hasCountDelayReason("SecurityDelay")(flyDF)

          val CountAirlineDelay =
            isAction()
              .hasCountDelayReason("AirlineDelay")(flyDF)                                                 

          val CountLateAircraftDelay = 
            isAction()
              .hasCountDelayReason("LateAircraftDelay")(flyDF)

          val CountWeatherDelay = 
            isAction()
              .hasCountDelayReason("WeatherDelay")(flyDF)


          val sumAirSystemDelay = 
            isAction()
              .hasSumDelayReason("AirSystemDelay")(flyDF)

          val sumSecurityDelay = 
            isAction()
              .hasSumDelayReason("SecurityDelay")(flyDF)

          val sumAirlineDelay = 
            isAction()
              .hasSumDelayReason("AirlineDelay")(flyDF)                                                 

          val sumLateAircraftDelay = 
            isAction()
              .hasSumDelayReason("LateAircraftDelay")(flyDF)

          val sumWeatherDelay = 
            isAction()
              .hasSumDelayReason("WeatherDelay")(flyDF)


          val hasCountDelay: Array[Long] = 
              Array(CountAirSystemDelay, 
                    CountSecurityDelay, 
                    CountAirlineDelay, 
                    CountLateAircraftDelay, 
                    CountWeatherDelay)

          val hasSumDelay: Array[Long] = 
              Array(sumAirSystemDelay, 
                    sumSecurityDelay, 
                    sumAirlineDelay, 
                    sumLateAircraftDelay, 
                    sumWeatherDelay)  


          val topAirportDF = flyDF
            .transform(isPreprocess()
              .hasTargetColumns(HasAirportColumns))
            .transform(isPreprocess()
              .hasNoNullValues)
            .transform(getTop10ByKey(HasTargetCancelFly, 
                                    "airportID", 
                                    airportDF))
            .transform(isTransformer()
              .withUpdateTable(ArchiveTopAirportDF))
            .transform(getGroupAndOrder)

          val topAirlinesDF = flyDF
            .transform(isPreprocess()
              .hasTargetColumns(HasAirlineColumns))
            .transform(isPreprocess()
              .hasNoNullValues)
            .transform(getTop10ByKey(HasTargetDepartureDelay, 
                                    "airlineID", 
                                    airlineDF))
            .transform(isTransformer()
              .withUpdateTable(ArchiveTopAirlinesDF))
            .transform(getGroupAndOrder)

          val topFlyInOneDirectionByAirportDF = flyDF
            .transform(isPreprocess()
              .hasTargetColumns(HasDestinationColumns))
            .transform(isPreprocess()
              .hasNoNullValues)
            .transform(isTransformer()
              .hasCountTwoGroupsByKey)
            .transform(isTransformer()
              .hasOrderedDataByColumn("count"))
            .transform(isTransformer()
              .withReducedNumberOfRows(10))
            .transform(isTransformer()
              .withConcatNameCol(airportDF, 
                                "airportID", 
                                "DestinationAiport"))
            .transform(isTransformer()
              .withUpdateTable(ArchiveTopFlyDF))
            .transform(getGroupAndOrder)

          val topWeekDaysByArrivalDelayDF = flyDF
            .transform(isPreprocess()
              .hasTargetColumns(HasDayWeekColumns))
            .transform(isTransformer()
              .hasCountOneGroupByKey("DayOfWeek"))
            .transform(isTransformer()
              .hasOrderedDataByColumn("count"))
            .transform(isTransformer()
              .withUpdateTable(ArchiveTopWeekDaysDF))
            .transform(isTransformer()
              .hasSumValuesByKey("DayOfWeek", "count"))

          val delayByReasonDF = flyDF
            .transform(isPreprocess()
              .hasTargetColumns(HasDelayColumns))
            .transform(isTransformer()
              .hasTargetedData(HasTargetDelay))

          val countDelayReasonDF = delayByReasonDF
            .transform(isTransformer()
              .withCountDealyReason(hasCountDelay))
            .transform(isTransformer()
              .withUpdateTable(ArchiveCountDelayDF))
            .transform(isTransformer()
              .hasSumValuesByGroupKey)

          val getPercentageDelayReasonDF = delayByReasonDF
            .transform(isTransformer()
              .withPercentageDealyReason(hasSumDelay)) 
            .transform(isTransformer()
              .withUpdatePercentTable(ArchivePercentageDelayDF))
            .transform(isTransformer().hasSumValuesPercent)
            .transform(isTransformer().withPercentageForArchive)
          
          isWriteGetPercentageDelayReason().writegetPercentageDelayReason(getPercentageDelayReasonDF)
          isWriteTopAirlines().writeAirlines(topAirlinesDF)
          isWriteTopAirport().writeAirport(topAirportDF)
          isWriteTopFlyInOneDirectionByAirport().writetopFlyInOneDirectionByAirport(topFlyInOneDirectionByAirportDF)
          isWriteTopWeekDaysByArrivalDelay().writeTopWeekDaysByArrivalDelay(topWeekDaysByArrivalDelayDF)
          isWriteCountDelayReason().writeCountDelayReason(countDelayReasonDF)
          isWriteMetaInfo().writeMetaInfo(metaInfoDF)
    }
  }
}
    
