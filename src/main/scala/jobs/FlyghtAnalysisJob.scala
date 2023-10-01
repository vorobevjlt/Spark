package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Column 
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import readers.DataFrameTableReader
import readers.DataFrameReader
import transformers.DataFrameTableTransformer
import writers.DataFrameTableWriter
import writers.DataFrameWriter
import preprocessing.DataFrameTableCleaner
import metrics.DataFrameTableAction
import constants.{ConstantTarget, ConstantColumns}

case class JobConfig(
    airportReaderConfig: DataFrameTableReader.ReadConfig,
    flyReaderConfig: DataFrameTableReader.ReadConfig, 
    airlineReaderConfig: DataFrameTableReader.ReadConfig,
    topAirlinesConfig: DataFrameTableReader.ReadConfig,
    topAirportConfig: DataFrameTableReader.ReadConfig,
    topFlyInOneDirectionByAirportConfig: DataFrameTableReader.ReadConfig,
    topWeekDaysByArrivalDelayConfig: DataFrameTableReader.ReadConfig,
    countDelayReasonConfig: DataFrameTableReader.ReadConfig,
    percentageDelayReasonConfig: DataFrameTableReader.ReadConfig,
    metaInfoConfig: DataFrameTableReader.ReadConfig,
    writeAirportConfig: DataFrameTableWriter.WriteConfig,
    writeAirlinesConfig: DataFrameTableWriter.WriteConfig,
    writetopFlyInOneDirectionByAirportConfig: DataFrameTableWriter.WriteConfig,
    writeTopWeekDaysByArrivalDelayConfig: DataFrameTableWriter.WriteConfig,
    writeCountDelayReasonConfig: DataFrameTableWriter.WriteConfig,
    writegetPercentageDelayReasonConfig: DataFrameTableWriter.WriteConfig,
    writeMetaInfoConfig: DataFrameTableWriter.WriteConfig
    )

class FlyghtAnalysisJob (
    spark: SparkSession, 
    config: JobConfig ) extends Job{

   override def run(): Unit = {

      val getAirport = {
        new DataFrameTableReader(
          spark, 
          config.airportReaderConfig)
      }

      val getfly = {
        new DataFrameTableReader(
          spark, 
          config.flyReaderConfig)
      }

      val getAirline = {
        new DataFrameTableReader(
          spark, 
          config.airlineReaderConfig)
      }

      val getArchiveTopAirlines = {
        new DataFrameTableReader(
          spark, 
          config.topAirlinesConfig)
      }

      val getArchiveTopAirport = {
        new DataFrameTableReader(
          spark, 
          config.topAirportConfig)
      }

      val getArchiveFlyInOneDirectionByAirport = {
        new DataFrameTableReader(
          spark, 
          config.topFlyInOneDirectionByAirportConfig)
      }

      val getArchiveTopWeekDays = {
        new DataFrameTableReader(
          spark, 
          config.topWeekDaysByArrivalDelayConfig)
      }

      val getArchiveCountDelayReason = {
        new DataFrameTableReader(
          spark, 
          config.countDelayReasonConfig)
      }

      val getArchivePercentageDelayReason = {
        new DataFrameTableReader(
          spark, 
          config.percentageDelayReasonConfig)
      }

      val getArchiveMetaInfo = {
        new DataFrameTableReader(
          spark, 
          config.metaInfoConfig)
      }

      val writeTopAirport = {
        new DataFrameTableWriter(
          spark, 
          config.writeAirportConfig)
      }

      val writeTopAirlines = {
        new DataFrameTableWriter(
          spark, 
          config.writeAirlinesConfig)
      }

      val writeTopFlyInOneDirectionByAirport = {
        new DataFrameTableWriter(
          spark, 
          config.writetopFlyInOneDirectionByAirportConfig)
      }
      val writeTopWeekDaysByArrivalDelay = {
        new DataFrameTableWriter(
          spark, 
          config.writeTopWeekDaysByArrivalDelayConfig)
      }
      val writeCountDelayReason = {
        new DataFrameTableWriter(
          spark, 
          config.writeCountDelayReasonConfig)
      }
      val writeGetPercentageDelayReason = {
        new DataFrameTableWriter(
          spark, 
          config.writegetPercentageDelayReasonConfig)
      }
      val writeMetaInfo = {
        new DataFrameTableWriter(
          spark, 
          config.writeMetaInfoConfig)
      }

      def isTransformer(): DataFrameTableTransformer = {
        new DataFrameTableTransformer()
      }

      def isAction(): DataFrameTableAction = {
        new DataFrameTableAction()
      }

      def isPreprocess(): DataFrameTableCleaner = {
        new DataFrameTableCleaner()
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
              .dropNoNullValues)
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
              .dropNoNullValues)
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
              .dropNoNullValues)
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
    
