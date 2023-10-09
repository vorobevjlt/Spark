package com.example

import configs.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import readers.DataFrameReader
import schemas.Schema
import writers.DataFrameWriter
import jobs.{Job, JobConfig, FlyghtAnalysisJob}

object FlightAnalyzer extends Config with SessionWrapper {

  val file = Array("a")

  def main(args: String): Unit = {
    Config(file)
  }

}  

