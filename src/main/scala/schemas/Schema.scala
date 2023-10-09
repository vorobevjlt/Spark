package schemas

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Schema {

val schemaFly = StructType(Seq(
        StructField("YEAR", IntegerType),
        StructField("MONTH", IntegerType),
        StructField("DAY", IntegerType),
        StructField("DayOfWeek", IntegerType),
        StructField("airlineID", StringType),
        StructField("FlightNumber", StringType),
        StructField("TailNumber", StringType),
        StructField("airportID", StringType),
        StructField("DestinationAiport", StringType),
        StructField("ScheduleDeparture", StringType),
        StructField("DepartureTime", IntegerType),
        StructField("DepartureDelay", IntegerType),
        StructField("TaxiOut", IntegerType),
        StructField("WheelsOff", IntegerType),
        StructField("ScheduleTime", IntegerType),
        StructField("ElipseTime", IntegerType),
        StructField("AirTime", IntegerType),
        StructField("DISTANCE", IntegerType),
        StructField("WheelsOn", IntegerType),
        StructField("TaxiIn", IntegerType),
        StructField("SCHEDULED_ARRIVAL", IntegerType),
        StructField("ArrivalTime", IntegerType),
        StructField("ArrivalDelay", IntegerType),
        StructField("DIVERTED", IntegerType),
        StructField("CANCELLED", IntegerType),
        StructField("CancelReason", StringType),
        StructField("AirSystemDelay", IntegerType),
        StructField("SecurityDelay", IntegerType),
        StructField("AirlineDelay", IntegerType),
        StructField("LateAircraftDelay", IntegerType),
        StructField("WeatherDelay", IntegerType),
    ))

      val schemaAirline = StructType(Seq(
        StructField("airlineID", StringType),
        StructField("name", StringType),
    ))

      val schemaAirport = StructType(Seq(
        StructField("airportID", StringType),
        StructField("name", StringType)
    ))

    val schemaTopAirport = StructType(Seq(
        StructField("name", StringType),
        StructField("count", LongType)
    ))

    val schemaTopAirline = StructType(Seq(
        StructField("name", StringType),
        StructField("count", LongType)
    ))

    val schemaTopFlyInOneDirection = StructType(Seq(
        StructField("name", StringType),
        StructField("count", LongType)
    ))

    val schemaTopWeekDays = StructType(Seq(
        StructField("DayOfWeek", IntegerType),
        StructField("count", LongType)

    ))
    
    val schemaCountDelayReason = StructType(Seq(
        StructField("AirSystemDelayCount", LongType),
        StructField("SecurityDelayCount", LongType),
        StructField("AirlineDelayCount", LongType),
        StructField("LateAircraftDelayCount", LongType),
        StructField("WeatherDelayCount", LongType)
    ))
    
    val schemaPercentageDelayReason = StructType(Seq(
        StructField("AirSystemDelayPercentage", DoubleType),
        StructField("SecurityDelayPercentage", DoubleType),
        StructField("AirlineDelayPercentage", DoubleType),
        StructField("LateAircraftDelayPercentage", DoubleType),
        StructField("WeatherDelayCountPercentage", DoubleType),
        StructField("AirSystemDelaySum", LongType),
        StructField("SecurityDelaySum", LongType),
        StructField("AirlineDelaySum", LongType),
        StructField("LateAircraftDelaySum", LongType),
        StructField("WeatherDelayCountSum", LongType),
        StructField("SumDelayReason", DoubleType)
    ))

    val schemaMetaInfo = StructType(Seq(
        StructField("collected", StringType),
        StructField("processed", StringType)
    ))

    }