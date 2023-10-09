package configs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import readers.DataFrameReader
import schemas.Schema
import writers.DataFrameWriter
import jobs.{Job, JobConfig, FlyghtAnalysisJob}

trait Config {
    val readHeader: Boolean = true
    val setSeparator: Char = ','
    // val env = sys.env("HOME")
    val env = "/root"
    val readPath = "src/main/resources/dataForRead/"
    val readArchive = "src/main/resources/dataArchiveForRead/"
}

object Config extends Config with SessionWrapper{
    private object LocalConfig extends Config {
         
            val job = new FlyghtAnalysisJob(
                spark,
                JobConfig(
                DataFrameReader.ReadConfig(
                    file = s"$readPath airports.csv}",
                    schema = Schema.schemaAirport,
                    hasHeader = readHeader,
                    separator = setSeparator),

                DataFrameReader.ReadConfig(
                    file = s"${readPath}flights.csv",
                    schema = Schema.schemaFly,
                    hasHeader = readHeader,
                    separator = setSeparator),

                DataFrameReader.ReadConfig(
                    file = s"${readPath}airlines.csv",
                    schema = Schema.schemaAirline,
                    hasHeader = readHeader,
                    separator = setSeparator),

                DataFrameReader.ReadConfig(
                    file = s"${readArchive}TopAirlines",
                    schema = Schema.schemaTopAirline,
                    hasHeader = readHeader,
                    separator = setSeparator),

                DataFrameReader.ReadConfig(
                    file = s"${readArchive}TopAirport",
                    schema = Schema.schemaTopAirport,
                    hasHeader = readHeader,
                    separator = setSeparator),

                DataFrameReader.ReadConfig(
                    file = s"${readArchive}TopFlyInOneDirectionByAirport",
                    schema = Schema.schemaTopFlyInOneDirection,
                    hasHeader = readHeader,
                    separator = setSeparator),

                DataFrameReader.ReadConfig(
                    file = s"${readArchive}TopWeekDaysByArrivalDelay",
                    schema = Schema.schemaTopWeekDays,
                    hasHeader = readHeader,
                    separator = setSeparator),

                DataFrameReader.ReadConfig(
                    file = s"${readArchive}CountDelayReason",
                    schema = Schema.schemaCountDelayReason,
                    hasHeader = readHeader,
                    separator = setSeparator),

                DataFrameReader.ReadConfig(
                    file = s"${readArchive}PercentageDelayReason",
                    schema = Schema.schemaPercentageDelayReason,
                    hasHeader = readHeader,
                    separator = setSeparator),

                DataFrameReader.ReadConfig(
                    file = s"${readArchive}MetaInfo",
                    schema = Schema.schemaMetaInfo,
                    hasHeader = false,
                    separator = setSeparator),

                DataFrameWriter.WriteConfig(
                    outputFile = "src/main/resources/dataArchiveForWrite/TopAirport",
                    format = "parquet"),

                DataFrameWriter.WriteConfig(
                    outputFile = "src/main/resources/dataArchiveForWrite/TopAirlines",
                    format = "parquet"),

                DataFrameWriter.WriteConfig(
                    outputFile = "src/main/resources/dataArchiveForWrite/TopFlyInOneDirectionByAirport",
                    format = "parquet"),

                DataFrameWriter.WriteConfig(
                    outputFile = "src/main/resources/dataArchiveForWrite/TopWeekDaysByArrivalDelay",
                    format = "parquet"),

                DataFrameWriter.WriteConfig(
                    outputFile = "src/main/resources/dataArchiveForWrite/CountDelayReason",
                    format = "parquet"),

                DataFrameWriter.WriteConfig(
                    outputFile = "src/main/resources/dataArchiveForWrite/PercentageDelayReason",
                    format = "parquet"),

                DataFrameWriter.WriteConfig(
                    outputFile = "src/main/resources/dataArchiveForWrite/MetaInfo",
                    format = "csv")
                )
                )
            job.run()
        }

        // private object ClasterConfig extends Config {
        //         val job = new FlyghtAnalysisJob(
        //         spark,
        //         JobConfig(
        //             DataFrameReader.ReadConfig(
        //             file = args(0),
        //             schema = Schema.schemaAirport,
        //             hasHeader = true,
        //             separator = ','),

        //             DataFrameReader.ReadConfig(
        //             file = args(1),
        //             schema = Schema.schemaFly,
        //             hasHeader = true,
        //             separator = ','),

        //             DataFrameReader.ReadConfig(
        //             file = args(2),
        //             schema = Schema.schemaAirline,
        //             hasHeader = true,
        //             separator = ','),

        //             DataFrameReader.ReadConfig(
        //             file = args(3),
        //             schema = Schema.schemaTopAirline,
        //             hasHeader = true,
        //             separator = ','),

        //             DataFrameReader.ReadConfig(
        //             file = args(4),
        //             schema = Schema.schemaTopAirport,
        //             hasHeader = true,
        //             separator = ','),

        //             DataFrameReader.ReadConfig(
        //             file = args(5),
        //             schema = Schema.schemaTopFlyInOneDirection,
        //             hasHeader = true,
        //             separator = ','),

        //             DataFrameReader.ReadConfig(
        //             file = args(6),
        //             schema = Schema.schemaTopWeekDays,
        //             hasHeader = true,
        //             separator = ','),

        //             DataFrameReader.ReadConfig(
        //             file = args(7),
        //             schema = Schema.schemaCountDelayReason,
        //             hasHeader = true,
        //             separator = ','),

        //             DataFrameReader.ReadConfig(
        //             file = args(8),
        //             schema = Schema.schemaPercentageDelayReason,
        //             hasHeader = true,
        //             separator = ','),

        //         DataFrameReader.ReadConfig(
        //             file = args(9),
        //             schema = Schema.schemaMetaInfo,
        //             hasHeader = false,
        //             separator = ','),

        //             DataFrameWriter.WriteConfig(
        //             outputFile = "/opt/spark-data/dataArchiveForWrite/TopAirport",
        //             format = "parquet"),

        //             DataFrameWriter.WriteConfig(
        //             outputFile = "/opt/spark-data/dataArchiveForWrite/TopAirlines",
        //             format = "parquet"),

        //             DataFrameWriter.WriteConfig(
        //             outputFile = "/opt/spark-data/dataArchiveForWrite/TopFlyInOneDirectionByAirport",
        //             format = "parquet"),

        //             DataFrameWriter.WriteConfig(
        //             outputFile = "/opt/spark-data/dataArchiveForWrite/TopWeekDaysByArrivalDelay",
        //             format = "parquet"),

        //             DataFrameWriter.WriteConfig(
        //             outputFile = "/opt/spark-data/dataArchiveForWrite/CountDelayReason",
        //             format = "parquet"),

        //             DataFrameWriter.WriteConfig(
        //             outputFile = "/opt/spark-data/dataArchiveForWrite/PercentageDelayReason",
        //             format = "parquet"),

        //             DataFrameWriter.WriteConfig(
        //             outputFile = "/opt/spark-data/dataArchiveForWrite/MetaInfo",
        //             format = "csv")
        //             ) 
        //         )
        //         job.run()
        // }

    def apply(
            args: Array[String],
            envType: String = env): Unit = {
        args match {
            case name if (name.nonEmpty && envType == "/root") => LocalConfig(name)
            case name if (name.isEmpty && envType != "/root") => println("root")
            case _ => throw new IllegalArgumentException(s"Invalid")
        }
    }
}