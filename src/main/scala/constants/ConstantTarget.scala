package constants

import org.apache.spark.sql.functions._

object ConstantTarget {

    val HasTargetCancelFly = col("CANCELLED") === 0
    
    val HasTargetDepartureDelay = col("DepartureDelay") === 0

    val HasTargetArrivalDelay = col("ArrivalDelay") === 0

    val HasTargetDelay = ((col("AirSystemDelay") =!= 0)
                        || (col("SecurityDelay") =!= 0)
                        || (col("AirlineDelay") =!= 0)
                        || (col("LateAircraftDelay") =!= 0)
                        || (col("WeatherDelay") =!= 0))

}
