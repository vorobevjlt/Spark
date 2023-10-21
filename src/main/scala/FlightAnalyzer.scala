package com.example

import configs.Config
import jobs.{Job, JobConfig, FlyghtAnalysisJob}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import writers.DataFrameWriter
import readers.DataFrameReader
import schemas.Schema

object FlightAnalyzer extends Config with SessionWrapper{

    def main(args: Array[String]): Unit = {
        require(args.length == 10 || args.length == 0 ,"Not enough files")    
        
        val readPath = args match {
        case items if (items.length == 10) => items
        case _ => localPath
        }

        val job = new FlyghtAnalysisJob(
                        spark,
                        JobConfig(
                        DataFrameReader.ReadConfig(
                            file = readPath(0),
                            schema = Schema.schemaAirport,
                            hasHeader = isHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(1),
                            schema = Schema.schemaFly,
                            hasHeader = isHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(2),
                            schema = Schema.schemaAirline,
                            hasHeader = isHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(3),
                            schema = Schema.schemaTopAirline,
                            hasHeader = isHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(4),
                            schema = Schema.schemaTopAirport,
                            hasHeader = isHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(5),
                            schema = Schema.schemaTopFlyInOneDirection,
                            hasHeader = isHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(6),
                            schema = Schema.schemaTopWeekDays,
                            hasHeader = isHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(7),
                            schema = Schema.schemaCountDelayReason,
                            hasHeader = isHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(8),
                            schema = Schema.schemaPercentageDelayReason,
                            hasHeader = isHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(9),
                            schema = Schema.schemaMetaInfo,
                            hasHeader = false,
                            separator = setSeparator),

                        DataFrameWriter.WriteConfig(
                            outputFile = writePath(0),
                            format = writeFormat),

                        DataFrameWriter.WriteConfig(
                            outputFile = writePath(1),
                            format = writeFormat),

                        DataFrameWriter.WriteConfig(
                            outputFile = writePath(2),
                            format = writeFormat),

                        DataFrameWriter.WriteConfig(
                            outputFile = writePath(3),
                            format = writeFormat),

                        DataFrameWriter.WriteConfig(
                            outputFile = writePath(4),
                            format = writeFormat),

                        DataFrameWriter.WriteConfig(
                            outputFile = writePath(5),
                            format = writeFormat),

                        DataFrameWriter.WriteConfig(
                            outputFile = writePath(6),
                            format = "csv")
                        )
        )
        job.run()
    }
}
