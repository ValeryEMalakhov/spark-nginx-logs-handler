import sbt._

//==============Versions===============
object V {
  // CDH
  lazy val sparkCDH      = "1.6.0-cdh5.12.0"
  lazy val hiveCDH       = "1.1.0-cdh5.12.0"
  lazy val hadoopCDH     = "2.6.0-cdh5.12.0"

  lazy val scopt         = "3.7.0"

  lazy val typesafe      = "1.3.2"
  lazy val slf4j         = "2.1.2"
}

object Dependencies {
  lazy val sparkCore      = "org.apache.spark" %% "spark-core"      % V.sparkCDH
  lazy val sparkSQL       = "org.apache.spark" %% "spark-sql"       % V.sparkCDH
  lazy val sparkStreaming = "org.apache.spark" %% "spark-streaming" % V.sparkCDH
  lazy val sparkHive      = "org.apache.spark" %% "spark-hive"      % V.sparkCDH

  lazy val spark = Seq(
      sparkCore
    , sparkSQL
    , sparkStreaming
    , sparkHive
  )

  lazy val hadoopCommon  = "org.apache.hadoop" % "hadoop-common"    % V.hadoopCDH
  lazy val hadoopClient  = "org.apache.hadoop" % "hadoop-client"    % V.hadoopCDH
  lazy val hadoopHdfs    = "org.apache.hadoop" % "hadoop-hdfs"      % V.hadoopCDH

  lazy val hadoop = Seq(
      hadoopCommon
    , hadoopClient
    , hadoopHdfs
  )

  lazy val configType = "com.typesafe" % "config" % V.typesafe
  lazy val slf4j      = "com.typesafe.scala-logging" %% "scala-logging-slf4j" % V.slf4j

  lazy val scopt = "com.github.scopt" %% "scopt"  % V.scopt
}

