package com.example
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import readers.CsvReader
import schemas.LineSchema
import writers.DataFrameTableWriter
import jobs.{Job, JobConfig}

object FlightAnalyzer extends SessionWrapper {

  def main(args: Array[String]): Unit = {

    val job = new Job(
      spark,
      JobConfig(

        CsvReader.ReadConfig(
          file = args(0),
          schema = LineSchema.schemaAirport,
          hasHeader = true,
          separator = ','),

        CsvReader.ReadConfig(
          file = args(1),
          schema = LineSchema.schemaFly,
          hasHeader = true,
          separator = ','),

        CsvReader.ReadConfig(
          file = args(2),
          schema = LineSchema.schemaAirline,
          hasHeader = true,
          separator = ','),

        CsvReader.ReadConfig(
          file = args(3),
          schema = LineSchema.schemaTopAirline,
          hasHeader = true,
          separator = ','),

        CsvReader.ReadConfig(
          file = args(4),
          schema = LineSchema.schemaTopAirport,
          hasHeader = true,
          separator = ','),

        CsvReader.ReadConfig(
          file = args(5),
          schema = LineSchema.schemaTopFlyInOneDirection,
          hasHeader = true,
          separator = ','),

        CsvReader.ReadConfig(
          file = args(6),
          schema = LineSchema.schemaTopWeekDays,
          hasHeader = true,
          separator = ','),

        CsvReader.ReadConfig(
          file = args(7),
          schema = LineSchema.schemaCountDelayReason,
          hasHeader = true,
          separator = ','),

        CsvReader.ReadConfig(
          file = args(8),
          schema = LineSchema.schemaPercentageDelayReason,
          hasHeader = true,
          separator = ','),

      CsvReader.ReadConfig(
        file = args(9),
        schema = LineSchema.schemaMetaInfo,
        hasHeader = false,
        separator = ','),

        DataFrameTableWriter.WriteConfig(
          outputFile = "/opt/spark-data/dataArchiveForWrite/TopAirport",
          format = "parquet"),

        DataFrameTableWriter.WriteConfig(
          outputFile = "/opt/spark-data/dataArchiveForWrite/TopAirlines",
          format = "parquet"),

        DataFrameTableWriter.WriteConfig(
          outputFile = "/opt/spark-data/dataArchiveForWrite/TopFlyInOneDirectionByAirport",
          format = "parquet"),

        DataFrameTableWriter.WriteConfig(
          outputFile = "/opt/spark-data/dataArchiveForWrite/TopWeekDaysByArrivalDelay",
          format = "parquet"),

        DataFrameTableWriter.WriteConfig(
          outputFile = "/opt/spark-data/dataArchiveForWrite/CountDelayReason",
          format = "parquet"),

        DataFrameTableWriter.WriteConfig(
          outputFile = "/opt/spark-data/dataArchiveForWrite/PercentageDelayReason",
          format = "parquet"),

        DataFrameTableWriter.WriteConfig(
          outputFile = "/opt/spark-data/dataArchiveForWrite/MetaInfo",
          format = "csv")
        ) 
      )
    job.run()
  }
}


object Dev extends App with SessionWrapper {

  val job = new Job(
    spark,
    JobConfig(

      CsvReader.ReadConfig(
        file = "src/main/resources/dataForRead/airports.csv",
        schema = LineSchema.schemaAirport,
        hasHeader = true,
        separator = ','),

      CsvReader.ReadConfig(
        file = "src/main/resources/dataForRead/flights.csv",
        schema = LineSchema.schemaFly,
        hasHeader = true,
        separator = ','),

      CsvReader.ReadConfig(
        file = "src/main/resources/dataForRead/airlines.csv",
        schema = LineSchema.schemaAirline,
        hasHeader = true,
        separator = ','),

      CsvReader.ReadConfig(
        file = "src/main/resources/dataArchiveForRead/TopAirlines",
        schema = LineSchema.schemaTopAirline,
        hasHeader = true,
        separator = ','),

      CsvReader.ReadConfig(
        file = "src/main/resources/dataArchiveForRead/TopAirport",
        schema = LineSchema.schemaTopAirport,
        hasHeader = true,
        separator = ','),

      CsvReader.ReadConfig(
        file = "src/main/resources/dataArchiveForRead/TopFlyInOneDirectionByAirport",
        schema = LineSchema.schemaTopFlyInOneDirection,
        hasHeader = true,
        separator = ','),

      CsvReader.ReadConfig(
        file = "src/main/resources/dataArchiveForRead/TopWeekDaysByArrivalDelay",
        schema = LineSchema.schemaTopWeekDays,
        hasHeader = true,
        separator = ','),

      CsvReader.ReadConfig(
        file = "src/main/resources/dataArchiveForRead/CountDelayReason",
        schema = LineSchema.schemaCountDelayReason,
        hasHeader = true,
        separator = ','),

      CsvReader.ReadConfig(
        file = "src/main/resources/dataArchiveForRead/PercentageDelayReason",
        schema = LineSchema.schemaPercentageDelayReason,
        hasHeader = true,
        separator = ','),

      CsvReader.ReadConfig(
        file = "src/main/resources/dataArchiveForRead/MetaInfo",
        schema = LineSchema.schemaMetaInfo,
        hasHeader = false,
        separator = ','),

      DataFrameTableWriter.WriteConfig(
        outputFile = "src/main/resources/dataArchiveForWrite/TopAirport",
        format = "parquet"),

      DataFrameTableWriter.WriteConfig(
        outputFile = "src/main/resources/dataArchiveForWrite/TopAirlines",
        format = "parquet"),

      DataFrameTableWriter.WriteConfig(
        outputFile = "src/main/resources/dataArchiveForWrite/TopFlyInOneDirectionByAirport",
        format = "parquet"),

      DataFrameTableWriter.WriteConfig(
        outputFile = "src/main/resources/dataArchiveForWrite/TopWeekDaysByArrivalDelay",
        format = "parquet"),

      DataFrameTableWriter.WriteConfig(
        outputFile = "src/main/resources/dataArchiveForWrite/CountDelayReason",
        format = "parquet"),

      DataFrameTableWriter.WriteConfig(
        outputFile = "src/main/resources/dataArchiveForWrite/PercentageDelayReason",
        format = "parquet"),

      DataFrameTableWriter.WriteConfig(
        outputFile = "src/main/resources/dataArchiveForWrite/MetaInfo",
        format = "csv")
    )
      
    )

  job.run()

}