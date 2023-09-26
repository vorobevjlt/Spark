package constants

import org.apache.spark.sql.functions._

class Constant {

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

    val HasTargetCancelFly = col("CANCELLED") === 0
    
    val HasTargetDepartureDelay = col("DepartureDelay") === 0

    val HasTargetArrivalDelay = col("ArrivalDelay") === 0

    val HasTargetDelay = ((col("AirSystemDelay") =!= 0)
                        || (col("SecurityDelay") =!= 0)
                        || (col("AirlineDelay") =!= 0)
                        || (col("LateAircraftDelay") =!= 0)
                        || (col("WeatherDelay") =!= 0))

    val HasColumnsForGroup = Seq[String](  
        "AirSystemDelayCount", 
        "SecurityDelayCount",
        "AirlineDelayCount",
        "LateAircraftDelayCount",
        "WeatherDelayCount")
}
