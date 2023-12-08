## sbt project compiled with Scala 3

### Usage

This is a normal sbt project. You can compile code with `sbt compile`, run it with `sbt run`, and `sbt console` will start a Scala 3 REPL.

docker-compose up --scale spark-worker=3

docker exec -it spark-cluster_spark-master_1 bash

./spark/bin/spark-submit --class com.example.FlightAnalyzer --deploy-mode client --master spark://060b3fb76d8e:7077 --verbose --supervise /opt/spark-apps/spark-stepik_2.12-0.1.jar /opt/spark-data/airports.csv /opt/spark-data/fli.csv /opt/spark-data/airlines.csv /opt/spark-data/dataArchiveForRead2/TopAirlines /opt/spark-data/dataArchiveForRead2/TopAirport /opt/spark-data/dataArchiveForRead2/TopFlyInOneDirectionByAirport /opt/spark-data/dataArchiveForRead2/TopWeekDaysByArrivalDelay /opt/spark-data/dataArchiveForRead2/CountDelayReason /opt/spark-data/dataArchiveForRead2/PercentageDelayReason /opt/spark-data/dataArchiveForRead2/MetaInfo


Показатели для расчета

    топ-10 самых популярных аэропортов по количеству совершаемых полетов
    топ-10 авиакомпаний, вовремя выполняющих рейсы
    для каждого аэропорта найти топ-10 перевозчиков и аэропортов назначения на основании вовремя совершенного вылета
    дни недели в порядке своевременности прибытия рейсов, совершаемых в эти дни 
    сколько рейсов были на самом деле задержаны по причине  AIR_SYSTEM_DELAY / SECURITY_DELAY / AIRLINE_DELAY / LATE_AIRCRAFT_DELAY / WEATHER_DELAY
    для каждой причины (AIR_SYSTEM_DELAY / SECURITY_DELAY / AIRLINE_DELAY / LATE_AIRCRAFT_DELAY / WEATHER_DELAY) рассчитайте процент от общего количества минут задержки рейсов

При поступлении новых данных идет обновление показателей метрик на основе уже имеющихся и далее итоги новых расчетов вносятся в архив.
Для вычислений используется Standalone кластер, в котором один мастер и три воркера, запускаемых в докере.
   
