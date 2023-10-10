package configs

import constants.ConstantPath
import jobs.{Job, JobConfig, FlyghtAnalysisJob}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import writers.DataFrameWriter
import readers.DataFrameReader
import schemas.Schema

trait Config {
    val readHeader: Boolean = true
    val setSeparator: Char = ','
    val localPath = ConstantPath.HasReadPath
    val writePath = ConstantPath.HasWritePath
    val writeFormat = "parquet"
}

object Config extends Config with SessionWrapper{
    def apply(args: Array[String] = Array()): Unit = {
        args match {
            case rec if(rec.nonEmpty) => RunConfig(args)
            case _ => RunConfig(localPath)       
        }
    }

    private object RunConfig extends Config {
        def apply(readPath: Array[String]): Unit = {
            val job = new FlyghtAnalysisJob(
                        spark,
                        JobConfig(
                        DataFrameReader.ReadConfig(
                            file = readPath(0),
                            schema = Schema.schemaAirport,
                            hasHeader = readHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(1),
                            schema = Schema.schemaFly,
                            hasHeader = readHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(2),
                            schema = Schema.schemaAirline,
                            hasHeader = readHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(3),
                            schema = Schema.schemaTopAirline,
                            hasHeader = readHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(4),
                            schema = Schema.schemaTopAirport,
                            hasHeader = readHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(5),
                            schema = Schema.schemaTopFlyInOneDirection,
                            hasHeader = readHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(6),
                            schema = Schema.schemaTopWeekDays,
                            hasHeader = readHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(7),
                            schema = Schema.schemaCountDelayReason,
                            hasHeader = readHeader,
                            separator = setSeparator),

                        DataFrameReader.ReadConfig(
                            file = readPath(8),
                            schema = Schema.schemaPercentageDelayReason,
                            hasHeader = readHeader,
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
}