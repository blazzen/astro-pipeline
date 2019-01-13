name := "azureastropipeline"
version := "1.0"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
).map(_.excludeAll(
  ExclusionRule("log4j"),
  ExclusionRule("slf4j-log4j12")
))

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.3.0",
  "org.apache.spark" % "spark-mllib_2.11" % "2.3.0",
  "org.jmockit" % "jmockit" % "1.34" % "test"
)

libraryDependencies += "gov.nasa.gsfc.heasarc" % "nom-tam-fits" % "1.12.0"
libraryDependencies += "io.github.malapert" % "JWcs" % "1.2.0" from "file://" + baseDirectory.value + "/lib/JWcs-1.2.0/target/JWcs-1.2.0.jar"
libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"
