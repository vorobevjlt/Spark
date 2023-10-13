name := "Flights Analyzer"

version := "0.1"

// scalaVersion := "2.13.8"
scalaVersion := "2.12.15"


lazy val sparkVersion = "3.2.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
SettingKey[Option[String]]("ide-package-prefix") := Option("com.example")