package constants

import org.apache.spark.sql.functions._

object ConstantColumns {

    val HasDayWeekColumns = Seq[String](  
        "DayOfWeek",
        "DepartureDelay"
        )

    val HasAirportColumns = Seq[String](  
        "airportID", 
        "CANCELLED")

    val HasAirlineColumns = Seq[String](  
        "airlineID", 
        "DepartureDelay")

    val HasDestinationColumns = Seq[String](  
        "airportID", 
        "DestinationAiport")

    val HasDelayColumns = Seq[String](  
        "AirSystemDelay",
        "SecurityDelay",
        "AirlineDelay",
        "LateAircraftDelay",
        "WeatherDelay")

    val HasColumnsForGroup = Seq[String](  
        "AirSystemDelayCount", 
        "SecurityDelayCount",
        "AirlineDelayCount",
        "LateAircraftDelayCount",
        "WeatherDelayCount")
}
