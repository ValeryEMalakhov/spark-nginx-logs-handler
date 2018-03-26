import sbt._

//==============Versions===============
object V {
  lazy val spark         = "2.2.0"
  lazy val hive          = "2.3.2"
  lazy val hadoop        = "2.9.0"
  lazy val hbase         = "1.4.2"
  lazy val parquet       = "1.9.0"

  lazy val scalazStream  = "0.8.6"
  lazy val scalazCore    = "7.2.18"

  lazy val typesafe      = "1.3.2"
  lazy val scopt         = "3.7.0"

  lazy val slf4j         = "3.7.2"
  lazy val cats          = "1.0.1"
  lazy val json          = "2.9.4"

  //  Tests  //
  lazy val specs2        = "4.0.2"
  lazy val scalatest     = "3.0.5"

  // CDH
  lazy val hiveCDH       = "1.1.0-cdh5.12.0"
  lazy val hadoopCDH     = "2.6.0-cdh5.12.0"
  lazy val hbaseCDH      = "1.2.0-cdh5.12.0"
  lazy val jetty         = "6.1.26"
}

object Dependencies {
  lazy val sparkCore      = "org.apache.spark" %% "spark-core"      % V.spark// % "provided"
  lazy val sparkSQL       = "org.apache.spark" %% "spark-sql"       % V.spark// % "provided"
  lazy val sparkStreaming = "org.apache.spark" %% "spark-streaming" % V.spark// % "provided"
  lazy val sparkHive      = "org.apache.spark" %% "spark-hive"      % V.spark// % "provided"

  lazy val spark = Seq(
      sparkCore
    , sparkSQL
    , sparkStreaming
    , sparkHive
  )

  lazy val hadoopCommon  = "org.apache.hadoop" % "hadoop-common"    % V.hadoop % "provided"
  lazy val hadoopClient  = "org.apache.hadoop" % "hadoop-client"    % V.hadoop % "provided"
  lazy val hadoopHdfs    = "org.apache.hadoop" % "hadoop-hdfs"      % V.hadoop % "provided"

  lazy val hadoop = Seq(
      hadoopCommon
    , hadoopClient
    , hadoopHdfs
  )

  lazy val hbaseClient = "org.apache.hbase" % "hbase-client" % V.hbase
  lazy val hbaseCommon = "org.apache.hbase" % "hbase-common" % V.hbase

  lazy val hbase = Seq(
      hbaseClient
    , hbaseCommon
  )

  lazy val jettyCore      = "org.mortbay.jetty" % "jetty"           % V.jetty
  lazy val jettySSLEngine = "org.mortbay.jetty" % "jetty-sslengine" % V.jetty
  lazy val jettyUtil      = "org.mortbay.jetty" % "jetty-util"      % V.jetty

  lazy val jetty = Seq(
    jettyCore
    , jettySSLEngine
    , jettyUtil
  )

  lazy val parquetColumn = "org.apache.parquet" % "parquet-column" % V.parquet

  lazy val scalazStream  = "org.scalaz.stream" %% "scalaz-stream"  % V.scalazStream
  lazy val scalazCore    = "org.scalaz" %% "scalaz-core"           % V.scalazCore

  lazy val scalaz = Seq(
      scalazCore
    , scalazStream
  )

  lazy val configType = "com.typesafe" % "config" % V.typesafe
  lazy val slf4j      = "com.typesafe.scala-logging" %% "scala-logging" % V.slf4j

  lazy val scopt = "com.github.scopt" %% "scopt"  % V.scopt

  lazy val cats  = "org.typelevel" %% "cats-core" % V.cats

  lazy val jacksonDatabind    = "com.fasterxml.jackson.core" % "jackson-databind"        % V.json
  lazy val jacksonCore        = "com.fasterxml.jackson.core" % "jackson-core"            % V.json
  lazy val jacksonAnnotations = "com.fasterxml.jackson.core" % "jackson-annotations"     % V.json
  lazy val jacksonModule      = "com.fasterxml.jackson.module" %% "jackson-module-scala" % V.json

  lazy val json = Seq(
      jacksonDatabind
    , jacksonCore
    , jacksonAnnotations
    , jacksonModule
  )

  //  Tests  //
  lazy val specs2       = "org.specs2" %% "specs2-core"   % V.specs2
  lazy val specs2Common = "org.specs2" %% "specs2-common" % V.specs2
  lazy val specs2Mock   = "org.specs2" %% "specs2-mock"   % V.specs2
  lazy val scalatest    = "org.scalatest" %% "scalatest"  % V.scalatest

  lazy val sTest = Seq(
      specs2
    , specs2Common
    , specs2Mock
    , scalatest
  )
}

