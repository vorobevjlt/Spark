## sbt project compiled with Scala 3

### Usage

This is a normal sbt project. You can compile code with `sbt compile`, run it with `sbt run`, and `sbt console` will start a Scala 3 REPL.

For more information on the sbt-dotty plugin, see the
[scala3-example-project](https://github.com/scala/scala3-example-project/blob/main/README.md).

docker-compose up --scale spark-worker=3

./spark/bin/spark-submit --class com.example.FlightAnalyzer --deploy-mode client --master spark://060b3fb76d8e:7077 --verbose --supervise /opt/spark-apps/spark-stepik_2.12-0.1.jar /opt/spark-data/airports.csv /opt/spark-data/fli.csv /opt/spark-data/airlines.csv /opt/spark-data/dataArchiveForRead2/TopAirlines /opt/spark-data/dataArchiveForRead2/TopAirport /opt/spark-data/dataArchiveForRead2/TopFlyInOneDirectionByAirport /opt/spark-data/dataArchiveForRead2/TopWeekDaysByArrivalDelay /opt/spark-data/dataArchiveForRead2/CountDelayReason /opt/spark-data/dataArchiveForRead2/PercentageDelayReason /opt/spark-data/dataArchiveForRead2/MetaInfo
# SparkProject-FlightsAnalyze
