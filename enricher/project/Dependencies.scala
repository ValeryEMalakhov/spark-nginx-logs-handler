import sbt._

//==============Versions===============
object V {
  lazy val sparkVersion         = "2.2.0"
  lazy val hiveVersion          = "2.3.2"
  lazy val parquetVersion       = "1.9.0"
  lazy val scalazStreamVersion  = "0.8.6"
  lazy val scalazCoreVersion    = "7.2.18"
  lazy val typesafeVersion      = "1.3.2"
  lazy val scoptVersion         = "3.7.0"
  lazy val slf4jVersion         = "3.7.2"
  lazy val catsVersion          = "1.0.1"
}

object Dependencies {
  lazy val sparkCore      = "org.apache.spark" %% "spark-core"      % V.sparkVersion
  lazy val sparkSQL       = "org.apache.spark" %% "spark-sql"       % V.sparkVersion
  lazy val sparkStreaming = "org.apache.spark" %% "spark-streaming" % V.sparkVersion
  lazy val sparkHive      = "org.apache.spark" %% "spark-hive"      % V.sparkVersion

  lazy val parquetColumn  = "org.apache.parquet" % "parquet-column" % V.parquetVersion

  lazy val scalazStream   = "org.scalaz.stream" %% "scalaz-stream"  % V.scalazStreamVersion
  lazy val scalazCore     = "org.scalaz" %% "scalaz-core"           % V.scalazCoreVersion

  lazy val configType = "com.typesafe" % "config"     % V.typesafeVersion
  lazy val slf4j      = "com.typesafe.scala-logging"  %% "scala-logging" % V.slf4jVersion

  lazy val scopt  = "com.github.scopt" %% "scopt"   % V.scoptVersion

  lazy val cats   = "org.typelevel" %% "cats-core"  % V.catsVersion
}

