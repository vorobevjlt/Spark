package constants

import org.apache.spark.sql.functions._

object ConstantPath {

    val ReadPath = Array(
        "src/main/resources/dataArchiveForRead/airports.csv",
        "src/main/resources/dataArchiveForRead/flights.csv",
        "src/main/resources/dataArchiveForRead/airlines.csv",
        "src/main/resources/dataArchiveForRead/TopAirlines",
        "src/main/resources/dataArchiveForRead/TopAirport",
        "src/main/resources/dataArchiveForRead/TopFlyInOneDirectionByAirport",
        "src/main/resources/dataArchiveForRead/TopWeekDaysByArrivalDelay",
        "src/main/resources/dataArchiveForRead/CountDelayReason",
        "src/main/resources/dataArchiveForRead/PercentageDelayReason",
        "src/main/resources/dataArchiveForRead/MetaInfo"
    )

    val WritePath = Array(
        "src/main/resources/dataArchiveForWrite/TopAirlines",
        "src/main/resources/dataArchiveForWrite/TopAirport",
        "src/main/resources/dataArchiveForWrite/TopFlyInOneDirectionByAirport",
        "src/main/resources/dataArchiveForWrite/TopWeekDaysByArrivalDelay",
        "src/main/resources/dataArchiveForWrite/CountDelayReason",
        "src/main/resources/dataArchiveForWrite/PercentageDelayReason",
        "src/main/resources/dataArchiveForWrite/MetaInfo"
    )    
}
