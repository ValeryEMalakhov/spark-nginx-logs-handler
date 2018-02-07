import sbt._

object Dependencies {

  //==============Versions===============

  lazy val scala211 = "2.11.11"
  lazy val scala212 = "2.12.4"

  lazy val sparkVersion = "2.2.0"
  lazy val hiveVersion = "2.3.2"
  lazy val parquetVersion = "1.9.0"

  lazy val scalazStreamVersion = "0.8.6"
  lazy val scalazCoreVersion = "7.2.18"

  lazy val typesafeVersion = "1.3.2"
  lazy val scoptVersion = "3.7.0"

  lazy val slf4jVersion = "3.7.2"

  lazy val catsVersion = "1.0.1"

  //============Dependencies=============

  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  lazy val sparkSQL = "org.apache.spark" %% "spark-sql" % sparkVersion
  lazy val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion
  lazy val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion

  lazy val parquetColumn = "org.apache.parquet" % "parquet-column" % parquetVersion

  lazy val scalazStream = "org.scalaz.stream" %% "scalaz-stream" % scalazStreamVersion
  lazy val scalazCore = "org.scalaz" %% "scalaz-core" % scalazCoreVersion

  lazy val configType = "com.typesafe" % "config" % typesafeVersion
  lazy val slf4j = "com.typesafe.scala-logging" %% "scala-logging" % slf4jVersion

  lazy val scopt = "com.github.scopt" %% "scopt" % scoptVersion

  lazy val cats = "org.typelevel" %% "cats-core" % catsVersion
}

